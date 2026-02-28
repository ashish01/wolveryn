#!/bin/bash
set -euo pipefail

# Benchmark script: generate data, index, search, report
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
INDEXER="$ROOT_DIR/rust/target/release/indexer"
SEARCH_CLI="$ROOT_DIR/rust/target/release/search-cli"

NUM_DOCS="${1:-10000}"
BENCH_DIR="/tmp/search_bench_$$"

export SDKROOT="${SDKROOT:-/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk}"
export DEVELOPER_DIR="${DEVELOPER_DIR:-/Library/Developer/CommandLineTools}"

echo "=== Search Engine Benchmark ==="
echo "Documents: $NUM_DOCS"
echo "Working dir: $BENCH_DIR"
echo ""

mkdir -p "$BENCH_DIR"

# Step 1: Generate data
echo "--- Generating $NUM_DOCS documents ---"
time python3 "$SCRIPT_DIR/bench_gen.py" --docs "$NUM_DOCS" --output "$BENCH_DIR/docs.jsonl"
echo ""

# Step 2: Index
echo "--- Indexing ---"
time "$INDEXER" index --input "$BENCH_DIR/docs.jsonl" --output "$BENCH_DIR/index" --fields title,body
echo ""

# Step 3: Merge (if multiple segments)
SEGMENT_COUNT=$(ls -d "$BENCH_DIR/index/segment_"* 2>/dev/null | wc -l)
if [ "$SEGMENT_COUNT" -gt 1 ]; then
    echo "--- Merging $SEGMENT_COUNT segments ---"
    time "$INDEXER" merge --index "$BENCH_DIR/index"
    echo ""
fi

# Step 4: Index size
echo "--- Index size ---"
du -sh "$BENCH_DIR/index"
du -sh "$BENCH_DIR/index"/*
echo ""

# Step 5: Search queries
echo "--- Search Queries ---"

queries=(
    "algorithm"
    "search engine"
    "fast efficient"
    "machine learning"
    "data OR network"
    "system NOT security"
    "performance optimization algorithm"
    "the"
)

for q in "${queries[@]}"; do
    echo "  Query: \"$q\""
    "$SEARCH_CLI" --index "$BENCH_DIR/index" --query "$q" --limit 3 2>&1 | grep -E "(Found|results)" | head -1
    echo ""
done

# Step 6: Latency benchmark (repeated queries)
echo "--- Latency Benchmark (10 iterations each) ---"
for q in "algorithm" "search engine" "data OR network"; do
    echo -n "  \"$q\": "
    total=0
    for i in $(seq 1 10); do
        ms=$("$SEARCH_CLI" --index "$BENCH_DIR/index" --query "$q" --limit 10 2>&1 | grep "results in" | grep -oE '[0-9]+\.[0-9]+ms' | head -1)
        total=$(echo "$total + ${ms%ms}" | bc)
    done
    avg=$(echo "scale=2; $total / 10" | bc)
    echo "avg ${avg}ms"
done
echo ""

# Cleanup
echo "--- Cleanup ---"
rm -rf "$BENCH_DIR"
echo "Done!"
