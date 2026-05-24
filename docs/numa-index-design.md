# NUMA-Aware Inter-Node Index Design Notes

Date: 2026-05-24
Status: Active design exploration

## Problem

With coherent TL routing, YCSB-C (100% reads, 4 nodes) shows 1111ns p50 GET.
TopazSN (single-node, all reads cross-NUMA) shows 441ns. The bottleneck is
not cross-NUMA latency — it's **finding which node owns the key** (sequential
probing of 3 remote KeyStores, each a swiss table lookup).

### EC2 c6a data (4T/64MB, post-coherence fix)

| System     | YCSB-A p50 | YCSB-B p50 | YCSB-C p50 |
|------------|-----------|-----------|-----------|
| TopazTL    | 80ns      | 74ns      | 1111ns    |
| TopazSN    | 393ns     | 413ns     | 441ns     |
| TBB        | 463ns     | 672ns     | 699ns     |
| RocksDB    | 549ns     | 232ns     | 168ns     |
| CacheLib   | 2664ns    | 1180ns    | 968ns     |

TopazTL wins write-heavy by 8-33x over CacheLib. Read-only is within 15%
of CacheLib. But the gap to RocksDB (168ns) and TBB (699ns) is real.

### Key insight: probe cost, not cross-NUMA cost

- Single cross-NUMA KeyStore.Find: ~150-300ns (swiss table, multiple cache lines)
- Sequential probing of 1-3 remote nodes: 1111ns average
- TopazSN proves: single cross-NUMA access is ~441ns, acceptable

## Failed approaches

### Routing cache (per-thread, 1024 slots)
- Too small for 75K remote keys (4 nodes, 25K per node)
- Cache thrashes; adds overhead on hot path
- p99 on YCSB-A/B went from 1880ns to 3100ns

### Hash hint (h2 % nodeCount as first remote probe)
- Free computation, zero memory
- Wrong ~75% of the time for TL-partitioned workloads
- hash("ycsb_K") % 4 doesn't match K / 25000 (the loading partition)
- Marginal improvement on YCSB-C

### Topology-aware probe order
- Correct in principle (sort remote nodes by NUMA distance)
- No effect on c6a (symmetric topology: nodes 0,1 = socket 0, nodes 2,3 = socket 1)
- Would help on asymmetric topologies

## Design constraints

1. NUMA cache-friendliness != single-node cache-friendliness
   - Shared read data causes cache-line bouncing across sockets
   - Proven by the 4T anomaly (alignas(64) statistics on node 0)
2. The index must be cheaper than probing 3 remote KeyStores
3. Migration (SET ownership transfer) is rare; GET miss is frequent
4. Key placement is policy-dependent (TL routing), not hash-deterministic
5. Information (key-value data) has no natural node affinity — placement is a policy choice

## Architecture: intra-node + inter-node separation

- **Intra-node:** hashmap (CMap/swiss table) — fast local lookup, well-understood
- **Inter-node:** new data structure — the design problem

## Design directions under consideration

### A) Replicated routing table
- Each node has a local copy of key→node mappings
- 100K keys × 1 byte = 100KB per node, 400KB total (0.6% of 64MB cache)
- GET miss: one local read (0 cross-NUMA for routing)
- Migration: write N copies (3 remote writes) — rare, acceptable
- Risk: stale copies between migration and propagation

### B) Interleaved pages (offset IS the hint)
- Pages allocated round-robin across nodes: page_index % nodeCount = home node
- Physical address of the page encodes which node owns it
- The memory layout IS the routing
- Variants:
  - All pages interleaved (changes entire memory layout)
  - Dedicated interleaved routing page (small, just for metadata)
  - Interleaved staging page (keys land local first, migrate to hash-home later)

### C) Separated control/slots with wider SIMD groups
- Control bytes (metadata): per-node, local, AVX-512 groups of 64 slots
- Slots (data): shared/interleaved across nodes
- Local control bytes answer "is key here?" without cross-NUMA
- Slot index encodes the node → one remote data access
- Requires AVX-512 (or AVX2 with 32-slot groups)
- Fundamental rethinking of swiss table for NUMA

### D) Hybrid: TL placement + hash-deterministic initial insert
- First SET: place on hash(key) % nodeCount (the "home node")
- Subsequent SET from different node: migrate (TL routing)
- GET miss: probe home node first, then topology scan
- Hint is 100% accurate for initial placement, stale after migration
- Small routing cache handles the migrated minority
- Changes SET semantics for initial insert only

## Key design questions

1. Should the inter-node structure be a hint (probabilistic) or a guarantee (exact)?
2. Should it be per-node, shared, or replicated?
3. Should placement follow the hash (DHT-style) or remain TL with a tracking layer?
4. What's the update cost on migration? (rare operation, but must be correct)
5. Can the physical memory layout (interleaving) replace an explicit data structure?

## Observations for future reference

- Swiss table degradation from pre-fix duplicates: each node had ~100K entries
  in 65K slots (153% load factor). Coherence fix reduced to 25K (38%).
  This means pre-fix YCSB-A/B "fast" numbers were inflated by fake locality
  AND degraded by table bloat. Post-fix gains came from correctness + cleaner tables.
- TopazSN is faster than TopazTL on YCSB-C because zero probing cost outweighs
  cross-NUMA latency. This proves the bottleneck is discovery, not distance.
- CacheLib has no NUMA coherence model — it's a single shared address space.
  Our "true coherence" (each key on exactly one node, synchronous transfer)
  is architecturally stronger but pays the probe cost.
