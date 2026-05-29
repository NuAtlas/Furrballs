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

---

## Session 2025-05-25: Coherence variants, sentinels, probe cost analysis

### Eventual coherence with dedup (commit 59fdd94)
- SET: purely local insert, enqueue dedup check to SPSC ring buffer
- Worker: drain ring, erase duplicates from remote nodes
- Merge-on-drain: collapse ring to last-writer-wins map before processing
- Numbers (4T/64MB EC2 c6a):
  - A p50 get: 241ns, A p50 set: 330ns, A ops/s: 7.01M
  - B p50 get: 252ns
  - C p50 get: 290ns, C ops/s: 6.17M
- Strict coherence baseline: A p50 get: 80ns, C p50 get: 1085ns

### Strict coherence variant (StrictCoherence flag)
- SET probes remote nodes synchronously, erases key from previous owner
- Multiple crashes fixed:
  1. 3-way race between benchmark threads + DrainMigrations: fixed by skipping
     DrainMigrations when StrictCoherence=true
  2. ABA race in UpdateInPlace: Find returns key, remote thread erases it,
     UpdateInPlace CAS succeeds on zeroed slot (DataOffset=null) → memcpy crash
     Fixed by checking DataOffset!=null inside seqlock in CMap::UpdateInPlace
  3. Stale page pointers: CMap erase + page cleanup race with concurrent UpdateInPlace
     memcpy. Root cause: Find snapshot is stale by the time UpdateInPlace runs.
     The seqlock doesn't prevent ABA (erase then re-access with stale result).
- Strict coherence numbers after fixes:
  - A p50 get: 268ns, A ops/s: 3.46M, hit rate: 97.7% (vs 99.96% eventual)
  - C p50 get: 622ns
- The old 80ns strict coherence was unsound — only worked at 2T/2N, ABA race
  was latent. At 4T/4N with Zipfian contention it crashes or degrades.

### Sentinel routing attempt (commit 88ea797)
- Instead of tombstone on remote erase, leave routing hint in the CMap slot
- Implementation: MigrateAndLeaveSentinel zeroes value, stores destNode in DataOffset
- GET path checks sentinel after each probe miss, redirects to indicated node
- Result: A/B showed 80ns across all nodes (impressive but misleading)
  - Caused by duplicates: reads find local stale copies before dedup runs
  - Not true convergence — reading own last write, not latest global value
  - Dedup worker runs every 1-10ms, benchmark completes in 60ms
  - Most reads hit stale duplicates, sentinels never exercised for p50
- C (read-only): 1113ns — sentinels never created (no SETs), pure sequential probe
- Core issue: sentinels are useful for cold-key migration but useless for hot
  contended keys that keep being written

### Core insight: the problem is probe cost, not cross-NUMA latency
- Single cross-NUMA CMap Find: ~300ns (one remote swiss table lookup)
  - Breakdown: ~100ns cross-NUMA memory access + ~200ns CMap probe logic
    (hash, load ctrl bytes, SIMD match, fingerprint check, seqlock read)
- Sequential probing of 3 nodes: ~900ns (3x the single-probe cost)
- A bloom filter per node could eliminate remote probes for absent keys:
  - One remote memory access (~100ns) to check a few bits
  - If negative: skip node entirely (saves ~200ns of CMap probe)
  - If positive: do the full CMap Find (~300ns total)
  - False positive rate ~5% with 512 bits per node for 25K keys
  - Worst case same as now, average case much better

### The fundamental tension
- Cross-node write contention on the same key is the unsolved problem
- Single-writer keys: everything works great (80ns local, one remote probe)
- Multi-writer keys: duplicates (eventual) or migration thrashing (strict)
- Per-node cache with 80ns reads is trivial (just pin threads)
- The NUMA challenge is specifically: making remote reads fast
- On NUMA, routing lookup (~35ns) is a significant fraction of remote access (~100ns)
  Unlike distributed systems where routing (~50ns) is negligible vs network (~100us)
- Any routing information must either be local (replicated, potentially stale) or
  remote (accurate, but costs ~100ns to access) — can't escape NUMA latency

### Broadcast race idea (not yet implemented)
- With strict coherence (one key copy), use NodeJobs to probe all nodes in parallel
- First node to find the key writes result to shared slot (single-writer guarantee)
- No synchronization needed — just lifetime management for the result slot
- Challenge: NodeJob overhead (mutex + condition variable = ~5-50us) is too slow
  for latency-critical operations. Would need busy-spinning workers instead.
- Alternative: each node has a pinned spinning worker reading from a shared mailbox
  Latency: ~100-300ns. Burns a CPU core per node.

### Next step: per-node bloom filter
- The bloom filter directly addresses the probe cost problem
- Per node: ~512 bits (64 bytes, one cache line) for 25K keys at ~5% FPR
- On GET miss: check remote bloom (one cross-NUMA load, ~100ns)
  - If bloom says no: skip CMap Find (save ~200ns)
  - If bloom says maybe: do full CMap Find (~300ns)
- Bloom updated on SET (insert) and DrainMigrations (erase)
- For YCSB-C: 75% of remote probes are to nodes that don't have the key
  Bloom eliminates these in ~100ns instead of ~300ns
- This is engineering, not research, but it's the most correct idea we have

