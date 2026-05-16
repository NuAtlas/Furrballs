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

# 0. Raise FD limit (RocksDB opens many files across iterations)
ulimit -n 65536

# 1. NUMA topology
numactl --hardware > "$RESULTS_DIR/numa-topology-$TIMESTAMP.txt" 2>&1
lscpu >> "$RESULTS_DIR/numa-topology-$TIMESTAMP.txt" 2>&1

# 2. Start memcached (for MemcachedBench)
if ! pgrep -x memcached > /dev/null; then
  memcached -d -p 11211 -u nobody -m 64
  sleep 1
  echo "memcached started"
fi

# 3. Full benchmark suite — all systems, no NUMA binding
#    FurrBall manages its own NUMA, so numactl on the process is irrelevant.
#    This run gives: ARC vs LRU vs TBB vs memcached, all on the same hardware.
echo "--- Full benchmark suite (all systems) ---"
"$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_out="$RESULTS_DIR/all-systems-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/all-systems-$TIMESTAMP.log"

# 4. Baselines pinned to single NUMA node (fair comparison)
#    LRU/TBB/memcached have no NUMA awareness — binding shows best-case
#    for systems that don't manage topology.
echo "--- Baselines pinned to NUMA node 0 ---"
numactl --cpunodebind=0 --membind=0 \
  "$BUILD_DIR/Benchmark/FurrBench" \
  --benchmark_filter="BaselineBench|TBBBench|MemcachedBench" \
  --benchmark_out="$RESULTS_DIR/baselines-node0-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/baselines-node0-$TIMESTAMP.log"

# 4. NUMA topology benchmark — multi-threaded, per-node pinning
#    Tests: FurrBall ThreadLocal vs RoundRobin vs TBB (NUMA-blind)
#    Workloads: partitioned, shared, trace replay
#    This is the paper's core benchmark.
echo "--- NUMA topology benchmark ---"
"$BUILD_DIR/Benchmark/NUMABench" \
  --benchmark_out="$RESULTS_DIR/numabench-$TIMESTAMP.json" \
  --benchmark_out_format=json \
  --benchmark_repetitions=3 \
  2>&1 | tee "$RESULTS_DIR/numabench-$TIMESTAMP.log"

echo "=== $(date) Run complete ==="
echo "Results in $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"*$TIMESTAMP*
echo ""
echo "Copy results back:"
echo "  scp -i /home/ubuntu/.ssh/ec2-furrballs.pem ubuntu@<IP>:$RESULTS_DIR/*$TIMESTAMP* /tmp/ec2-results/"
