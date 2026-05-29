# Furrballs

A NUMA-aware, high-performance caching library written in C++20 under the `NuAtlas` namespace.

The core thesis contribution is using **NUMA topology as a first-class input to cache placement and eviction decisions** — per-page allocation places data on the NUMA node of the requesting thread, with **REMARC** (Reduction-Modeled Adaptive Replacement Cache) as the eviction and migration policy. REMARC reduces three access dimensions (recency, frequency, locality) into a unified 2D state space (S_local, S_remote), producing dual action scores (Evict, Migrate) via precomputed SIMD lookup tables scanned with pshufb. No published work combines NUMA topology with a multi-dimensional adaptive cache policy at the page level.

## Architecture

```
NuAtlas::FurrBall
  +-- REMARC                           2D state space: (S_local, S_remote) → Evict/Migrate
  +-- CMap                             Concurrent Swiss table (lock-free reads, CAS writes)
  +-- MemoryManager                    NUMA-aware + regular allocation
  +-- NodeJob                          Per-NUMA-node pinned worker thread
  +-- Statistics                       Atomic hit/miss/eviction counters
  +-- RocksDB                          Persistence (block cache disabled)

NuAtlas::Numatic                    Platform abstraction (Linux/Windows)
  +-- NumaticUnix.cpp               libnuma
  +-- NumaticWin.cpp                Windows NUMA APIs
```

**Layered model:**
- **L1 (hot):** REMARC-managed pages with NUMA-aware placement and cross-node key migration — the thesis contribution.
- **L2 (cold):** RocksDB backing store. Furrballs owns all caching.

## Release (v0.1-alpha)

Snapshot for archival. This version includes:

- **Core library** (Furrballs/): 2800 LOC C++20. REMARC eviction/migration policy with SIMD lookup tables, CMap concurrent Swiss table, NUMA-aware page allocation, RocksDB persistence, NodeJob per-node worker threading.
- **Benchmarks** (Benchmark/): Single-node microbenchmark (30+ variants, 5 workloads), 3-node NUMA simulation, ghost map study. Archived experimental variants are in `#if 0` sections with their results preserved in the companion papers.
- **Documentation** (docs/):
  - [whitepaper.qmd](docs/whitepaper.qmd) — Furrballs systems paper (Paper 1). Covers architecture, NUMA placement, Phase 1/2 results.
  - [remarc-paper.qmd](docs/remarc-paper.qmd) — REMARC algorithm paper (Paper 2). Covers framework, evaluation (16 findings), desire encoding, independence theorem. Status: research journal (living document).
  - Published HTML+PDF available at the [project site](https://furrballs.pages.dev).

**Validation:** Compiled and tested on GCC 13, Linux x86-64. Single-node benchmarks use synthetic workloads. Multi-node evaluation is simulation-based. Real hardware validation was performed on AWS c6i.metal (see whitepaper §5.18).

## Dependencies

- CMake 3.25+
- C++20 compiler (GCC 13+, Clang 17+)
- [vcpkg](https://vcpkg.io) for dependency management
  - `lz4` — compression
  - `rocksdb` — persistent backing store
- `libnuma-dev` (Linux) — NUMA API

## Building

```bash
# Set vcpkg location (once)
export VCPKG_ROOT=/path/to/vcpkg

# Configure (Linux GCC, debug)
cmake --preset linux-debug

# Build
cmake --build build/linux-debug
```

Other presets: `linux-release`, `linux-clang-debug`, `linux-clang-release`, `windows-debug`, `windows-release`.

## VM Testing

QEMU NUMA simulation scripts live at `~/vm/furrballs/` (outside the repo to avoid snapshot bloat):

```bash
~/vm/furrballs/furr.sh start    # Boot the VM
~/vm/furrballs/furr.sh shell    # SSH into it
~/vm/furrballs/furr.sh stop     # Shut down
```

## Roadmap

| Phase | Focus | Status |
|---|---|---|
| 1 | NUMA-aware core, key-based API, benchmark harness | Done |
| 2a | CMap + ConcurrentARC (concurrent Swiss table, ARC eviction) | Done |
| 2b | REMARC policy, migration-based eviction, RocksDB persistence | Done |
| 3 | Adaptive Memory Pooling (AMP) — dynamic pool growth/contraction | Planned |
| 4 | Server + Client + binary protocol | Planned |
| 5 | Publication — formal benchmark results and thesis | In progress |

## Coding Guidelines

- `const` on member functions and arguments wherever possible.
- `noexcept` by default; only mark throwing when explicitly intended.
- Multi-threading first. Use atomic types/operations when possible.
- Only the paging system allocates memory (through `MemoryManager`).
- Error codes over exceptions in the public API.
- Factories instead of throwing constructors — guarantee all constructed objects are valid.
- Destructors must never throw — cleanup must always be reliable and stable.
- Exceptions are only for unrecoverable errors. Single try/catch layer, no catch-all.
- Prioritize **performance and low latency** over all else.

### Platform Priorities

Unix (POSIX) and Windows. macOS is not a priority.
