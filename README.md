# Furrballs

A NUMA-aware, high-performance caching library written in C++20 under the `NuAtlas` namespace.

The core thesis contribution is using **NUMA topology as a first-class input to cache placement and eviction decisions** — per-page allocation places data on the NUMA node of the requesting thread, with ARC (Adaptive Replacement Cache) eviction, write-back dirty tracking, and RocksDB persistence. No published work combines NUMA topology with adaptive cache policy at the page level.

## Architecture

```
NuAtlas::FurrBall
 +-- ARCPolicy<size_t, Page*>       ARC eviction (t1, t2, b1, b2)
 +-- MemoryManager                  NUMA-aware + regular allocation
 +-- KeyStore                       unordered_map<string, KeyMeta>
 +-- NodeJob                        Per-NUMA-node pinned worker thread
 +-- Statistics                     Atomic hit/miss/eviction counters
 +-- RocksDB                        Persistence (block cache disabled)

NuAtlas::Numatic                    Platform abstraction (Linux/Windows)
  +-- NumaticUnix.cpp               libnuma
  +-- NumaticWin.cpp                Windows NUMA APIs
```

**Layered model:**
- **L1 (hot):** ARC-managed pages with NUMA-aware placement — the thesis contribution.
- **L2 (cold):** RocksDB backing store. Furrballs owns all caching.

## Current Status (numa-focus branch)

Phase 1 — NUMA-Aware Core. The library compiles and runs with:
- ARC cache policy with eviction callback and dirty page flushing
- Per-page allocation with NUMA-aware path
- RocksDB persistence with block cache disabled
- Numatic platform abstraction (Linux complete, Windows stub)
- NodeJob per-node worker threading
- Key-based Set/Get API

Development and testing use a QEMU NUMA VM (2 nodes, 4 vCPUs, 4GB RAM).
See [docs/whitepaper.qmd](docs/whitepaper.qmd) for the full design rationale and roadmap.

## Dependencies

- CMake 3.25+
- C++17 compiler (GCC, Clang, MSVC)
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

| Phase | Focus |
|---|---|
| 1 (current) | NUMA-aware core, key-based API, benchmark harness |
| 2 | Per-page optimization (value reordering, cold migration) |
| 3 | Adaptive Memory Pooling (AMP) — dynamic pool growth/contraction |
| 4 | Server + Client + binary protocol |
| 5 | Publication — formal benchmark results and thesis |

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
