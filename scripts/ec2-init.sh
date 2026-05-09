#!/bin/bash
set -euo pipefail

echo "=== $(date) Furrballs EC2 init ==="

# 1. System packages
export DEBIAN_FRONTEND=noninteractive
apt update && apt install -y \
  build-essential g++-14 cmake git pkg-config \
  libnuma-dev numactl hwloc linux-tools-common \
  python3

update-alternatives --set cc /usr/bin/gcc-14 2>/dev/null || true
update-alternatives --set c++ /usr/bin/g++-14 2>/dev/null || true

# 2. NUMA topology (log before anything else)
echo "=== NUMA Topology ===" | tee /tmp/furrballs-numa.txt
numactl --hardware | tee -a /tmp/furrballs-numa.txt
lscpu | grep -E "NUMA|Socket|Core|Thread|Model name" | tee -a /tmp/furrballs-numa.txt

# 3. vcpkg
VCPKG_ROOT=/opt/vcpkg
if [ ! -d "$VCPKG_ROOT" ]; then
  git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT"
  "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics
fi
export VCPKG_ROOT
"$VCPKG_ROOT/vcpkg" install xxhash lz4 rocksdb --triplet x64-linux

# 4. Clone (public repo, numa-focus branch) and build
REPO_DIR=/opt/Furrballs
if [ ! -d "$REPO_DIR" ]; then
  git clone -b numa-focus https://github.com/NuAtlas/Furrballs.git "$REPO_DIR"
fi

BUILD_DIR="$REPO_DIR/buildrel"
mkdir -p "$BUILD_DIR"
cmake -S "$REPO_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build "$BUILD_DIR" --target Benchmark IsolatedBench MultiNodeBench -j$(nproc)

echo "=== $(date) Init complete ==="
echo "Build: $BUILD_DIR/Benchmark/"
echo "  - Benchmark/Benchmark (architecture/latency)"
echo "  - Benchmark/IsolatedBench (single-node, frozen tier)"
echo "  - Benchmark/MultiNodeBench (2-node, policy/migration)"
echo "NUMA:  /tmp/furrballs-numa.txt"
echo ""
echo "Run benchmarks:"
echo "  cd $BUILD_DIR && ./Benchmark/Benchmark"
echo "  cd $BUILD_DIR && ./Benchmark/IsolatedBench"
echo "  cd $BUILD_DIR && ./Benchmark/MultiNodeBench"
