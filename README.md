# Furrballs

NUMA-aware concurrent cache in C++20. Per-node lock-free Swiss tables (CMap), a per-node routing cache (annex), and pluggable eviction policies (LRU, ARC, S3-FIFO).

## Why

Production sharded caches (Memcached, Redis, CacheLib) all use LRU. Furrballs was built to understand why: policies that improve on LRU require shared state or contention-sensitive thresholds that degrade under per-node independence. LRU isn't the best policy — it's the only one among those tested that survives sharding without modification.

See [`docs/technical-note.qmd`](docs/technical-note.qmd) for the full analysis and `docs/whitepaper.qmd`](docs/whitepaper.qmd) for the complete design history.

## Results

AWS c6a.metal, AMD EPYC 7R13, 4 NUMA nodes, 192 vCPUs. GCC 14, Release.

### vs CacheLib (Meta)

NUMABench: 64 MB usable cache, 64 B values, 2M universe, Partitioned, Zipfian theta=0.99.

| Threads | CacheLib ops/s | FurrBall LRU ops/s | Speedup |
|---------|--------------|-------------------|---------|
| 4 | 5.1M | 16.0M | 3.1x |
| 8 | 5.7M | 8.7M | 1.5x |
| 16 | 6.0M | 16.0M | 2.7x |
| 32 | 9.3M | 23.9M | 2.6x |

YCSB: 4 threads, 64 MB cache, 64 B values.

| Workload | CacheLib ops/s | CacheLib p50 GET | FurrBall ops/s | FurrBall p50 GET |
|----------|--------------|------------------|----------------|-------------------|
| A (50R/50W) | 0.7M | 2,774ns | 16.0M | 60ns |
| B (95R/5W) | 1.7M | 1,213ns | 16.8M | 60ns |
| C (100R) | 2.2M | 1,014ns | 17.3M | 50ns |

CacheLib configured with tuned allocation classes, no NUMA binding. CacheLib requires 128 MB footprint (slab overhead) for 64 MB usable; FurrBall uses 64 MB. Full configuration in the technical note Appendix.

### Shard-Native Policy Analysis (32 threads, theta=0.90)

| Policy | Hit Rate | Issue |
|--------|----------|-------|
| LRU | 63.4% | No shared state — fully independent per node |
| S3-FIFO | 35.4% | Small queue (10% cap) turns over too fast under contention |
| ARC | 20.3% | Per-node p_ diverges from globally optimal value |

### S3-FIFO Lock-Free Reads (32 threads)

| Metric | LRU | S3-FIFO |
|--------|-----|---------|
| p50 GET | 910ns | **190ns** |
| p99 GET | 1,620ns | **1,270ns** |

S3-FIFO uses `FindWithAux()` for atomic counter increment during lookup, eliminating read-path lock contention.

## Architecture

```
NuAtlas::FurrBall<Policy>
  ├── CMap<Value>            Lock-free Swiss table (SSE2 probing, seqlock reads, aux[7])
  ├── Policy                 Compile-time template: LRU · ARC · S3-FIFO
  ├── Annex                  Per-node routing cache (key → owning node + data offset)
  ├── MemoryManager          NUMA-aware page allocation per node
  ├── NodeJob                Per-NUMA-node pinned maintenance worker
  ├── Statistics             Atomic per-node hit/miss/eviction counters
  └── RocksDB (optional)     Cold tier with block cache disabled

NuAtlas::Numatic            Platform abstraction
  ├── NumaticUnix.cpp        libnuma
  └── NumaticWin.cpp         Windows NUMA APIs
```

### CMap

Open-addressed Swiss table with 16-slot SSE2 SIMD probing groups. Lock-free reads via seqlock protocol (stamped version check, retry on writer interference). Each 64-byte aligned slot reserves 7 bytes of `aux` for policy scratchpad — CMap zeroes `aux` on insert and never reads or writes it. Policies use `FindWithAux()` and `EvictWithAux()` for single-probe combined read-and-policy operations.

### Annex (Routing Cache)

On a SET to node X, a hint `{nodeId, dataOffset, dataSize}` is batched and broadcast to all other nodes' annex indices. On a GET miss on the local node, the annex resolves the owning node in one lookup and reads directly from the stored offset — no probing of remote CMap instances.

This eliminates the cross-node probe cascade (O(N) remote probes → O(1) annex lookup). The architecture generalizes to RDMA: replace the `memcpy` from `dataOffset` with a one-sided RDMA read verb, and the annex becomes a local routing table for remote memory.

### Policies

Compile-time selectable via `FurrBall<Policy>` template. Each policy owns its key store, admission, and eviction. No virtual dispatch on the hot path.

## Build

Requirements: CMake 3.25+, C++20 compiler (GCC 13+, Clang 17+), vcpkg. Linux needs libnuma-dev.

```bash
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset linux-release
cmake --build build/linux-release
```

vcpkg dependencies: `benchmark`, `xxhash`, `rocksdb[tbb,lz4]`, `lz4`.

Presets: `linux-debug`, `linux-release`, `linux-clang-debug`, `linux-clang-release`, `windows-debug`, `windows-release`.

## Benchmarks

- `Benchmark/NUMABench.cpp` — NUMA topology benchmark. Adapters for FurrBall (TL/SN), TBB, CacheLib, CacheLib-Numa, RocksDB. Per-thread latency vectors for local-vs-remote analysis.
- `Benchmark/YCSBBench.cpp` — YCSB A/B/C against the same adapters, with capacity and thread scaling.
- `bench/ec2-run-all.sh` — Reproducible EC2 run script (environment capture, full matrix, table parser). ~15 min, ~$0.80 on c6a.metal.
- `data/ec2-c6a/` — Raw results (Google Benchmark JSON + plain text).

## Coding Guidelines

- `const` / `noexcept` by default. Exceptions only for unrecoverable errors.
- Atomics and lock-free types preferred. No virtual dispatch in hot paths.
- Error codes in public API. Factories over throwing constructors.
- Destructors never throw.
- Performance and latency above all else.

## Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | NUMA-aware core, key-based API, benchmark harness | Done |
| 2 | CMap Swiss table; LRU/ARC/S3-FIFO policies; annex routing cache | Done |
| 3 | Dynamic memory pooling | Planned |
| 4 | Server + client + binary protocol | Planned |
| 5 | RDMA transport via annex | Planned |

Platform support: Linux x86-64 (primary), Windows (in progress). macOS is not a target.
