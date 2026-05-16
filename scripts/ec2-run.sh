#!/bin/bash
set -euo pipefail

REPO_DIR=/opt/Furrballs
BUILD_DIR="$REPO_DIR/buildrel"
RESULTS_DIR="$REPO_DIR/results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

mkdir -p "$RESULTS_DIR"

export FURRBALL_TRACE="$REPO_DIR/data/twitter_cluster52.csv"

echo "=== $(date) FurrBench EC2 run ==="
echo "Trace: $FURRBALL_TRACE"
echo "Results: $RESULTS_DIR/"

# 1. NUMA topology
numactl --hardware > "$RESULTS_DIR/numa-topology-$TIMESTAMP.txt" 2>&1
lscpu >> "$RESULTS_DIR/numa-topology-$TIMESTAMP.txt" 2>&1

# 2. Start memcached (for MemcachedBench)
if ! pgrep -x memcached > /dev/null; then
  memcached -d -p 11211 -u nobody -m 64
  sleep 1
  echo "memcached started"
fi

# 3. Run FurrBench — all benchmarks, JSON output
echo "--- Running FurrBench (all benchmarks) ---"
"$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_out="$RESULTS_DIR/furrbench-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/furrbench-$TIMESTAMP.log"

# 4. Per-Numa-node run (bind to node 0, then node 1)
echo "--- Running on NUMA node 0 ---"
numactl --cpunodebind=0 --membind=0 \
  "$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_filter="FurrBench" \
  --benchmark_out="$RESULTS_DIR/furrbench-node0-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/furrbench-node0-$TIMESTAMP.log"

echo "--- Running on NUMA node 1 ---"
numactl --cpunodebind=1 --membind=1 \
  "$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_filter="FurrBench" \
  --benchmark_out="$RESULTS_DIR/furrbench-node1-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/furrbench-node1-$TIMESTAMP.log"

# 5. Cross-node (CPU on node 0, memory on node 1 — worst case)
echo "--- Running cross-NUMA (cpu=0, mem=1) ---"
numactl --cpunodebind=0 --membind=1 \
  "$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_filter="FurrBench" \
  --benchmark_out="$RESULTS_DIR/furrbench-cross-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/furrbench-cross-$TIMESTAMP.log"

echo "=== $(date) Run complete ==="
echo "Results in $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"*$TIMESTAMP*
echo ""
echo "Copy results back:"
echo "  scp -i /home/ubuntu/.ssh/ec2-furrballs.pem ubuntu@<IP>:$RESULTS_DIR/*$TIMESTAMP* /tmp/ec2-results/"
