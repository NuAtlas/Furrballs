#!/bin/bash
set -euo pipefail

echo "=== $(date) Furrballs EC2 c6i.metal init ==="

# 1. System packages
export DEBIAN_FRONTEND=noninteractive
apt update && apt install -y \
  build-essential g++-14 cmake git pkg-config \
  libnuma-dev numactl hwloc linux-tools-common \
  python3 libtbb-dev libmemcached-dev memcached \
  liblz4-dev libxxhash-dev autoconf automake libtool

update-alternatives --set cc /usr/bin/gcc-14 2>/dev/null || true
update-alternatives --set c++ /usr/bin/g++-14 2>/dev/null || true

# 2. NUMA topology (log before anything else)
echo "=== NUMA Topology ===" | tee /tmp/furrballs-numa.txt
numactl --hardware | tee -a /tmp/furrballs-numa.txt
lscpu | grep -E "NUMA|Socket|Core|Thread|Model name" | tee -a /tmp/furrballs-numa.txt

# 3. vcpkg (only for RocksDB — TBB/memcached from apt)
VCPKG_ROOT=/opt/vcpkg
if [ ! -d "$VCPKG_ROOT" ]; then
  git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT"
  "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics
fi
export VCPKG_ROOT
"$VCPKG_ROOT/vcpkg" install rocksdb benchmark --triplet x64-linux

# 4. Clone (public repo, numa-focus branch)
REPO_DIR=/opt/Furrballs
if [ ! -d "$REPO_DIR" ]; then
  git clone -b numa-focus https://github.com/NuAtlas/Furrballs.git "$REPO_DIR"
fi
cd "$REPO_DIR"
git pull

# 5. Trace will be scp'd from VPS (no reliable public URL)
TRACE_DIR="$REPO_DIR/data"
mkdir -p "$TRACE_DIR"
if [ ! -s "$TRACE_DIR/twitter_cluster52.csv" ]; then
  echo "WARN: No trace file at $TRACE_DIR/twitter_cluster52.csv"
  echo "      scp it from VPS: scp twitter_cluster52.csv ubuntu@<IP>:/tmp/"
  echo "      Then: sudo cp /tmp/twitter_cluster52.csv $TRACE_DIR/"
  echo "      Benchmarks will use Zipfian-only if trace is missing."
fi

# 6. Build FurrBench
BUILD_DIR="$REPO_DIR/buildrel"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cmake -S "$REPO_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build "$BUILD_DIR" --target FurrBench NUMABench -j$(nproc)

echo "=== $(date) Init complete ==="
echo "Build: $BUILD_DIR/Benchmark/FurrBench"
echo "       $BUILD_DIR/Benchmark/NUMABench"
echo "Trace: $TRACE_DIR/twitter_cluster52.csv"
echo "NUMA:  /tmp/furrballs-numa.txt"
echo ""
echo "Run:  sudo -E $REPO_DIR/scripts/ec2-run.sh"
