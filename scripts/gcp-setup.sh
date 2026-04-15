#!/bin/bash
set -euo pipefail

echo "=== Furrballs NUMA Benchmark Setup ==="

sudo apt-get update -qq
sudo apt-get install -y -qq \
    build-essential cmake ninja-build git \
    libnuma-dev pkg-config curl zip unzip tar \
    gdb valgrind

echo "libnuma-dev installed"

if ! command -v vcpkg &>/dev/null; then
    git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg
    /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
    echo 'export VCPKG_ROOT=/opt/vcpkg' >> /home/$USER/.bashrc
    export VCPKG_ROOT=/opt/vcpkg
fi

echo "=== Setup complete ==="
echo "VCPKG_ROOT=$VCPKG_ROOT"
echo ""
echo "To build Furrballs:"
echo "  git clone <your-repo> ~/source/repos/Furrballs"
echo "  cd ~/source/repos/Furrballs"
echo "  export VCPKG_ROOT=/opt/vcpkg"
echo "  cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release"
echo "  cmake --build build --target Benchmark"
echo "  ./build/Benchmark/Benchmark"
echo ""
echo "NUMA topology:"
numactl --hardware
