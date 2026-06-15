#!/usr/bin/env bash
# ec2-run-all.sh — Reproducible benchmark suite for Furrballs on EC2 c6a.metal
#
# Usage: ./bench/ec2-run-all.sh [output_dir]
#   output_dir defaults to data/ec2-c6a/<timestamp>
#
# Prerequisites (on EC2):
#   - SSH key for access
#   - vcpkg installed
#   - Furrballs repo cloned
#
# This script:
#   1. Records environment metadata (compiler, CPU, OS, cmake flags)
#   2. Builds NUMABench from clean
#   3. Runs the full benchmark matrix
#   4. Parses raw output into formatted tables
#   5. Saves everything for archival
#
# Cost: ~$3.21/hr on c6a.metal. Runtime ~15min.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTDIR="${1:-$REPO_ROOT/data/ec2-c6a/run-$(date +%Y%m%d-%H%M%S)}"
BENCH_BIN="$REPO_ROOT/buildfast/Benchmark/NUMABench"
MIN_TIME="5s"

mkdir -p "$OUTDIR"
RAW="$OUTDIR/raw.txt"
TABLE="$OUTDIR/tables.txt"

echo "=== Furrballs EC2 Benchmark Suite ==="
echo "Output: $OUTDIR"
echo "Date:   $(date -Iseconds)"
echo ""

# ---- Step 1: Environment metadata ----
{
    echo "=== ENVIRONMENT ==="
    echo "hostname: $(hostname)"
    echo "date:     $(date -Iseconds)"
    echo "os:       $(uname -r)"
    echo "cpu:      $(grep 'model name' /proc/cpuinfo | head -1)"
    echo "cpus:     $(nproc)"
    echo "numa:     $(lscpu | grep 'NUMA node(s)' || echo 'unknown')"
    echo "lscpu:"
    lscpu
    echo ""
    echo "compiler: $(g++ --version | head -1)"
    echo "cmake:    $(cmake --version | head -1)"
    echo ""
    echo "=== BUILD ==="
    echo "working_dir: $(pwd)"
    echo "branch:      $(git branch --show-current 2>/dev/null || echo 'detached')"
    echo "commit:      $(git rev-parse --short HEAD)"
    echo "status:"
    git status --short
    echo ""
    echo "=== CMAKE CACHE (relevant) ==="
    if [ -f "$REPO_ROOT/buildfast/CMakeCache.txt" ]; then
        grep -E "CMAKE_CXX_FLAGS|CMAKE_BUILD_TYPE" "$REPO_ROOT/buildfast/CMakeCache.txt" | head -10
    else
        echo "(no CMakeCache.txt)"
    fi
    echo ""
    echo "=== RAW BENCHMARK OUTPUT ==="
} > "$RAW"

# ---- Step 2: Build ----
echo "[build] Rebuilding NUMABench..."
cd "$REPO_ROOT"
if [ ! -f "buildfast/CMakeCache.txt" ]; then
    echo "[build] No CMakeCache found, doing full configure..."
    cmake -B buildfast -S . -DCMAKE_BUILD_TYPE=Release 2>&1 | tee -a "$RAW"
else
    echo "[build] Incremental build (existing buildfast/)"
fi
cmake --build buildfast --target NUMABench -j"$(nproc)" 2>&1 | tee -a "$RAW"

if [ ! -f "$BENCH_BIN" ]; then
    echo "FATAL: Build failed, $BENCH_BIN not found"
    exit 1
fi
echo "[build] OK"

# ---- Step 3: Run benchmarks ----

# Google Benchmark filter syntax: use / for literal match
# Args: {threads, capacityKB, workloadType, valueSize, zipfianUniverse, theta*100}
# workloadType: 0=Partitioned, 1=Shared, 2=Trace, 3=ReadOnly, 4=UniformRO
# Filter: SystemNameTL/Run/<threads>/<capacityKB>/<workload>/<valueSize>/<universe>/<theta>

run_bench() {
    local label="$1"
    local filter="$2"
    echo "[bench] $label (filter: $filter)"
    echo "" >> "$RAW"
    echo "--- $label ---" >> "$RAW"
    "$BENCH_BIN" --benchmark_filter="$filter" --benchmark_min_time="$MIN_TIME" \
        --benchmark_format=console --benchmark_repetitions=1 \
        >> "$RAW" 2>&1 || echo "WARNING: $label returned non-zero" >> "$RAW"
}

run_multi() {
    local label="$1"
    shift
    local filters=("$@")
    local combined=""
    for f in "${filters[@]}"; do
        if [ -n "$combined" ]; then combined+="|"; fi
        combined+="$f"
    done
    run_bench "$label" "$combined"
}

# ---- 3a: FurrBall core (ARC) vs LRU vs S3-FIFO, Partitioned (workload=0) ----
# 64MB cache, 2M universe, 64B values
for T in 4 8 16 32; do
    run_multi "ARC+LRU+S3FIFO Partitioned ${T}T" \
        "FurrBallTL/Run/${T}/65536/0/64/2000000/99" \
        "FurrBallLRUTL/Run/${T}/65536/0/64/2000000/99" \
        "FurrBallS3FIFOTL/Run/${T}/65536/0/64/2000000/99"
done

# ---- 3b: Shared (workload=1) for same configs ----
for T in 4 32; do
    run_multi "ARC+LRU+S3FIFO Shared ${T}T" \
        "FurrBallTL/Run/${T}/65536/1/64/2000000/99" \
        "FurrBallLRUTL/Run/${T}/65536/1/64/2000000/99" \
        "FurrBallS3FIFOTL/Run/${T}/65536/1/64/2000000/99"
done

# ---- 3c: Theta sweep (Partitioned, workload=0) ----
for T in 4 32; do
    for TH in 80 90 99; do
        run_multi "Theta Partitioned ${T}T theta=0.${TH}" \
            "FurrBallTL/Run/${T}/65536/0/64/2000000/${TH}" \
            "FurrBallLRUTL/Run/${T}/65536/0/64/2000000/${TH}" \
            "FurrBallS3FIFOTL/Run/${T}/65536/0/64/2000000/${TH}"
    done
done

# ---- 3d: Theta sweep (Shared, workload=1) ----
for T in 4 32; do
    for TH in 80 90 99; do
        run_multi "Theta Shared ${T}T theta=0.${TH}" \
            "FurrBallS3FIFOTL/Run/${T}/65536/1/64/2000000/${TH}" \
            "FurrBallLRUTL/Run/${T}/65536/1/64/2000000/${TH}"
    done
done

# ---- 3e: Competitors ----
for T in 4 8 16 32; do
    run_bench "TBB Partitioned ${T}T" \
        "TBB/Run/${T}/65536/0/64/2000000/99"
done

echo "" >> "$RAW"
echo "=== BENCHMARK RUN COMPLETE ===" >> "$RAW"
echo "[bench] All runs complete."

# ---- Step 4: Parse and format ----
echo ""
echo "[parse] Extracting results..."

python3 << 'PYEOF' "$RAW" "$TABLE"

import re, sys, math

raw_path = sys.argv[1]
table_path = sys.argv[2]

with open(raw_path) as f:
    lines = f.readlines()

results = []
current_section = "unknown"

for line in lines:
    m = re.match(r'--- (.+) ---', line)
    if m:
        current_section = m.group(1)
        continue

    if 'NUMABench_' not in line:
        continue

    hit = re.search(r'hit_rate_pct=([\d.]+)', line)
    ops = re.search(r'ops_per_sec=([\d.]+)M', line)
    p50 = re.search(r'p50_get_ns=([\d.]+k?)', line)
    p99 = re.search(r'p99_get_ns=([\d.]+k?)', line)
    runs = re.search(r'runs=(\d+)', line)

    if not hit:
        continue

    name_match = re.search(r'NUMABench_(\S+)', line)
    name = name_match.group(1) if name_match else "?"

    def parse_ns(s):
        if not s:
            return ""
        s = s.group(1)
        if s.endswith('k'):
            return f"{float(s[:-1])*1000:.0f}"
        return s

    results.append({
        'section': current_section,
        'name': name,
        'hit': float(hit.group(1)),
        'ops': float(ops.group(1)) if ops else None,
        'p50': parse_ns(p50),
        'p99': parse_ns(p99),
        'runs': int(runs.group(1)) if runs else 0,
    })

def fmt_ops(v):
    return f"{v:.1f}M" if v is not None else "  —  "

def fmt_p(v):
    return f"{v:>6s}" if v else "    —  "

def short_name(name):
    name = name.replace('FurrBall', '')
    name = name.replace('TL', '')
    name = name.replace('SN', '')
    name = name.replace('CN', '')
    name = name.replace('RR', '')
    name = name.replace('CacheLib', 'CLib')
    return name

with open(table_path, 'w') as out:
    out.write(f"Furrballs EC2 Benchmark Results\n")
    out.write(f"Generated: {results[0]['section'] if results else 'N/A'}\n")
    out.write(f"Total results: {len(results)}\n\n")

    # Group by section
    sections = {}
    for r in results:
        s = r['section']
        if s not in sections:
            sections[s] = []
        sections[s].append(r)

    for sec_name, sec_results in sections.items():
        out.write(f"{'='*80}\n")
        out.write(f" {sec_name}\n")
        out.write(f"{'='*80}\n")
        out.write(f"  {'System':<25s} {'hit%':>7s} {'ops/s':>9s} {'p50(ns)':>9s} {'p99(ns)':>9s} {'runs':>4s}\n")
        out.write(f"  {'-'*25} {'-'*7} {'-'*9} {'-'*9} {'-'*9} {'-'*4}\n")
        for r in sec_results:
            out.write(f"  {short_name(r['name']):<25s} {r['hit']:>7.1f} {fmt_ops(r['ops']):>9s} {fmt_p(r['p50']):>9s} {fmt_p(r['p99']):>9s} {r['runs']:>4d}\n")
        out.write("\n")

    # Summary: key comparisons
    out.write(f"{'='*80}\n")
    out.write(f" KEY COMPARISONS\n")
    out.write(f"{'='*80}\n\n")

    def find_first(system_sub, section_sub=None):
        for r in results:
            if system_sub in r['name']:
                if section_sub is None or section_sub in r['section']:
                    return r
        return None

    pairs = [
        ("LRU vs S3-FIFO 4T Partitioned", "LRUTL", "S3FIFOTL", "Partitioned 4T"),
        ("LRU vs S3-FIFO 32T Partitioned", "LRUTL", "S3FIFOTL", "Partitioned 32T"),
        ("LRU vs S3-FIFO 4T Shared", "LRUTL", "S3FIFOTL", "Shared 4T"),
        ("LRU vs S3-FIFO 32T Shared", "LRUTL", "S3FIFOTL", "Shared 32T"),
    ]

    for label, a_sub, b_sub, sec_sub in pairs:
        a = find_first(a_sub, sec_sub)
        b = find_first(b_sub, sec_sub)
        if a and b and a['ops'] and b['ops']:
            ratio = b['ops'] / a['ops'] if a['ops'] > 0 else float('nan')
            out.write(f"  {label}:\n")
            out.write(f"    LRU     : hit={a['hit']:.1f}%  ops={a['ops']:.1f}M/s  p99={a['p99']}ns\n")
            out.write(f"    S3-FIFO : hit={b['hit']:.1f}%  ops={b['ops']:.1f}M/s  p99={b['p99']}ns\n")
            out.write(f"    Ratio   : {ratio:.2f}x ops\n\n")

PYEOF

echo ""
echo "[done] Raw output: $RAW"
echo "[done] Tables:     $TABLE"
echo ""
echo "To shut down: sudo shutdown -h now"
