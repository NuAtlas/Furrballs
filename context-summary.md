# Furrballs Context Summary

## Project Overview
NUMA-aware high-performance caching library (C++20, MIT). Two-tier cache (L1: in-memory pages with pluggable eviction policy, L2: RocksDB persistence). Compile-time policy selection (LRU, ARC, S3-FIFO). Custom concurrent hash map (CMap, Swiss-table variant with SIMD probing + SeqLock lock-free reads). Per-NUMA-node sharded architecture with per-node pinned worker threads (NodeJob). ~10.5k LOC.

## Architecture
- Per-NUMA-node data sharding: each node has its own CMap + page pool + eviction policy instance
- Thread-local routing: writers pin keys to their local NUMA node; readers check local first
- Lock-free reads via SeqLock on CMap
- PromoteBuf: lock-free MPSC buffer for deferred ARC promotions
- FatAnnex (new default): per-node replicated routing index mapping key_hash -> (owning_node, data_ptr, data_size). Broadcast on Set via batched lock-free MPSC buffers; eliminates cross-node probe cascade on reads
- Blocked Bloom filter as optional remote-probe skip (alternative to FatAnnex)

## Current State
- FatAnnex is now default in multi-node mode (code in repo since late May 2026)
- EC2 benchmarks from May 17-18 are pre-FatAnnex (the old numbers show YCSB-C 3-14x worse than A/B)
- FatAnnex EC2 data exists on the EC2 instance (c6a) but hasn't been downloaded yet
- VPS (this machine) is single-node: 1 NUMA node, 6 cores, 11GB RAM. No multi-node testing possible here.
- S3-FIFO policy was added June 7

## Key Performance Claims (from memory, data on EC2)

### FatAnnex impact on YCSB workloads
- Before: YCSB-C (read-only) p50 latency 3-14x worse than YCSB-A/B on 2-4 NUMA nodes
- After (FatAnnex): YCSB-C latency matches YCSB-A/B within noise
- Cost: ~1% throughput regression on A/B workloads (annex broadcast overhead on Set)

### ARC vs LRU under concurrency
This is the central finding. ARC was tested in two modes on per-node sharded caches:

**Inline promotion:** ARC hit rate ≈ LRU hit rate, but throughput worse (policy-lock contention)
**Deferred/batched promotion:** ARC hit rate collapses. LRU hit rates are approximately:
- θ=0.80: LRU 48.3%, ARC ~14% (34pp gap)
- θ=0.99: LRU 95.5%, ARC ~70% (25pp gap)

LRU at 32 threads / 8 cores (θ=0.99): 25.2M ops/sec with 95.5% hit rate.

### S3-FIFO problems
Tested but not working well. With 32 threads on 8 cores sharing one SpinLock-protected S3FIFO instance, the Small queue (10% of capacity, ~16K slots) thrashes. New items flood in faster than they can be accessed twice for graduation to Main queue. Admission filter becomes a throughput bottleneck.

## Theoretical Findings / Open Questions

### ARC's concurrency problem is algorithmic, not just performance
ARC's ghost lists (B1/B2) give it adaptive edge over LRU via tight feedback loop: evict -> record in ghost -> next miss checks ghost -> adapt `p`. Under batching, that loop is delayed, ghost state becomes systematically wrong, and ARC degenerates to LRU with extra overhead. Even with perfect locking (inline), ARC offers no hit rate benefit over LRU while being slower.

The core issue: ARC was designed for single-threaded serialized access. Its `p` target derivation assumes causal consistency of ghost-state observations. Under concurrency, the `p` value is derived from causally inconsistent interleaved histories, making adaptation directionally wrong, not just noisy.

**The ARC Concurrency Dilemma:**
- Inline: correct algorithm, but policy-lock contention kills throughput
- Deferred: good throughput, but ghost-state drift makes ARC strictly worse than LRU

ARC is Pareto-dominated by LRU in concurrent settings: no regime where ARC wins.

### Deeper question: Is adaptation itself vacuous?
REMARC (the original custom policy) proved EMA-based scoring is zero-information. ARC's ghost adaptation is causally corrupted under concurrency. S3-FIFO's admission filter thrashes. The pattern suggests adaptation may be built on a false premise: that past inter-reference patterns predict future ones at the granularity the algorithm assumes.

Hypothesis: Key-level access patterns in concurrent workloads lack the temporal structure that adaptation exploits. All policies may achieve the same hit rate within noise when compared at equal throughput, and the "winning" policy is simply the one with lowest overhead (LRU).

### Endogenous traces: is Bélády's framework unsound for concurrent systems?
Even a perfect oracle (optimal hypothetical predictor) may be blind because the access sequence is endogenous. Thread A's access to key X changes the cache state, which affects whether Thread B's next access hits or misses, which changes B's scheduling delay, which changes when B issues its next access. The trace is a function of the policy. There's no ground-truth future to be optimal against — the "optimal" policy for one trace creates a different trace where it's suboptimal.

This suggests the entire optimality framework (compare policy hit rate against an oracle on a fixed trace) is unsound for concurrent systems. The real optimization target may not be hit rate but something like minimizing policy-induced inter-thread interference.

### Forward-looking questions
- What if adaptation itself (not just its implementation) is wrong?
- Bélády MIN may itself be useless under concurrency — not because it's unrealistic, but because the single-sequence model doesn't apply
- Multi-threaded access has semi-independent streams that interact through the policy in feedback loops
- Thread A affects Thread B, C, D, and even the future of A through B's state changes
- Maybe concurrent caches should not adapt at all, or need a fundamentally different kind of adaptation

## Benchmark Data Location
- Latest EC2 data (pre-FatAnnex): `/home/ubuntu/source/repos/Furrballs/data/ec2-c6a/`
- New FatAnnex data: on EC2 instance, needs download
- VPS sanity data: `data/ycsb-vps-sanity.json` (single-node, irrelevant for annex)
- Compiled tables: `data/full-benchmark-tables.txt` (292 rows, deduped summary)
- Full CSV: `data/full-benchmark-data.csv`

## Systems Compared
- FurrBall-TL (thread-local routing, default multi-node mode — FatAnnex carrier)
- FurrBall-SN (single-node simulated, no NUMA penalty)
- FurrBall-TL + LRU, ARC, S3-FIFO policies
- CacheLib (Meta's production cache)
- CacheLibNuma (CacheLib with per-pool NUMA routing)
- TBB (concurrent_hash_map)
- RocksDB (disk-backed)
- Various routing strategies: round-robin, cross-node, hash-routed