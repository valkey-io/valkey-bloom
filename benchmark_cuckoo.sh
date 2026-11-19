#!/usr/bin/env bash
# Cuckoo filter performance benchmark.
#
# Usage (after building the module and valkey):
#   SERVER_VERSION=unstable sh benchmark_cuckoo.sh
#
# Output: Markdown table printed to stdout, suitable for CUCKOO_IMPLEMENTATION_STATUS.md.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_VERSION="${SERVER_VERSION:-unstable}"
OS_TYPE=$(uname)

if [ "$OS_TYPE" = "Darwin" ]; then
    MODULE_EXT=".dylib"
else
    MODULE_EXT=".so"
fi

MODULE_PATH="${MODULE_PATH:-$SCRIPT_DIR/target/release/libvalkey_bloom$MODULE_EXT}"
SERVER_BIN="$SCRIPT_DIR/tests/build/binaries/$SERVER_VERSION/valkey-server"
BENCH_BIN="$SCRIPT_DIR/tests/build/binaries/$SERVER_VERSION/valkey-benchmark"
CLI_BIN="$SCRIPT_DIR/tests/build/binaries/$SERVER_VERSION/valkey-cli"

BENCH_PORT=16399
BENCH_REQUESTS=100000
BENCH_CLIENTS=50
BENCH_PIPELINE=1  # serial (pipeline=1); increase for throughput test

# ── Validation ─────────────────────────────────────────────────────────────

if [ ! -f "$MODULE_PATH" ]; then
    echo "ERROR: Module not found at $MODULE_PATH"
    echo "Run 'SERVER_VERSION=$SERVER_VERSION sh build.sh' first."
    exit 1
fi

if [ ! -x "$SERVER_BIN" ]; then
    echo "ERROR: valkey-server not found at $SERVER_BIN"
    echo "Run 'SERVER_VERSION=$SERVER_VERSION sh build.sh' first."
    exit 1
fi

if [ ! -x "$BENCH_BIN" ]; then
    echo "ERROR: valkey-benchmark not found at $BENCH_BIN"
    echo "Build valkey with 'make -j' inside tests/build/valkey."
    exit 1
fi

# ── Start server ────────────────────────────────────────────────────────────

echo "Starting valkey-server on port $BENCH_PORT ..." >&2
"$SERVER_BIN" \
    --port "$BENCH_PORT" \
    --loadmodule "$MODULE_PATH" \
    --daemonize yes \
    --logfile /tmp/valkey_bench_$BENCH_PORT.log \
    --pidfile /tmp/valkey_bench_$BENCH_PORT.pid

# Wait for server to be ready
for i in $(seq 1 20); do
    if "$CLI_BIN" -p "$BENCH_PORT" PING 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 0.2
done

if ! "$CLI_BIN" -p "$BENCH_PORT" PING 2>/dev/null | grep -q PONG; then
    echo "ERROR: Server did not start. Check /tmp/valkey_bench_$BENCH_PORT.log"
    exit 1
fi

cleanup() {
    echo "Stopping server ..." >&2
    "$CLI_BIN" -p "$BENCH_PORT" SHUTDOWN NOSAVE 2>/dev/null || true
}
trap cleanup EXIT

# ── Helpers ──────────────────────────────────────────────────────────────────

# run_bench <label> <command-string>
# Prints the requests/sec extracted from valkey-benchmark output.
run_bench() {
    local label="$1"
    local cmd="$2"
    local result
    result=$("$BENCH_BIN" \
        -p "$BENCH_PORT" \
        -n "$BENCH_REQUESTS" \
        -c "$BENCH_CLIENTS" \
        -P "$BENCH_PIPELINE" \
        --command "$cmd" \
        --csv 2>/dev/null | tail -1)
    # CSV format: "command","rps","avg_latency","min_latency","p50","p95","p99","max_latency"
    local rps
    rps=$(echo "$result" | cut -d',' -f2 | tr -d '"')
    printf "%s\t%s\n" "$label" "$rps"
}

# ── Pre-warm: create a large filter so exists/del have something to find ─────

echo "Pre-warming benchmark filter ..." >&2
"$CLI_BIN" -p "$BENCH_PORT" CF.RESERVE benchkey 500000 BUCKETSIZE 4 EXPANSION 1 > /dev/null
# Populate 100k items so CF.EXISTS hits are real
for i in $(seq 1 100000); do
    printf "CF.ADD benchkey item%d\r\n" "$i"
done | "$CLI_BIN" -p "$BENCH_PORT" --pipe > /dev/null 2>&1

# ── Run benchmarks ───────────────────────────────────────────────────────────

echo "Running benchmarks (n=$BENCH_REQUESTS, c=$BENCH_CLIENTS) ..." >&2

declare -A RESULTS

while IFS=$'\t' read -r label rps; do
    RESULTS["$label"]="$rps"
done < <(
    run_bench "CF.ADD (new key each op)"    "CF.ADD benchkey __rand_int__"
    run_bench "CF.EXISTS (populated filter)" "CF.EXISTS benchkey __rand_int__"
    run_bench "CF.RESERVE (unique key)"     "CF.RESERVE __rand_int__ 1000 BUCKETSIZE 4 EXPANSION 1"
)

# CF.DEL and CF.COUNT need to run against the pre-populated filter
while IFS=$'\t' read -r label rps; do
    RESULTS["$label"]="$rps"
done < <(
    run_bench "CF.DEL (populated filter)"   "CF.DEL benchkey __rand_int__"
    run_bench "CF.COUNT (populated filter)" "CF.COUNT benchkey __rand_int__"
)

# ── Print Markdown table ──────────────────────────────────────────────────────

echo ""
echo "### CF.* Command Throughput (n=$BENCH_REQUESTS, c=$BENCH_CLIENTS, pipeline=$BENCH_PIPELINE)"
echo ""
printf "| %-40s | %20s |\n" "Command" "Throughput (req/sec)"
printf "| %-40s | %20s |\n" "$(printf '%0.s-' {1..40})" "$(printf '%0.s-' {1..20})"

for label in \
    "CF.ADD (new key each op)" \
    "CF.EXISTS (populated filter)" \
    "CF.DEL (populated filter)" \
    "CF.COUNT (populated filter)" \
    "CF.RESERVE (unique key)"; do
    rps="${RESULTS[$label]:-N/A}"
    printf "| %-40s | %20s |\n" "$label" "$rps"
done
echo ""
echo "_Benchmark config: valkey-server $SERVER_VERSION, module $(basename $MODULE_PATH), $(uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) CPUs_"
echo ""
