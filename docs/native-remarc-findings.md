# NativeRemarc Findings

## Architecture

NativeRemarc is the **native mode** instantiation of the REMARC framework.
The scorer IS the cache — no ARC structure (no T1/T2/B1/B2/ghost/p-adaptation).
Direct eviction by desire score via min-heap.

### Components (`NativeStore.h`)
- `CMap<Value>` backend — swiss table, CAS-based lock-free reads
- `Entry[]` — open-addressing desire map (h2-indexed, backward-shift deletion)
  - `desire` (uint8, atomic) — recent interest proxy, decays by /2 per epoch
  - `freq` (uint8, atomic) — long-term popularity, capped at 31
  - `score` (uint16, atomic) — composite: `(desire << 4) | (freq/2)`, min 255
- `HeapEntry[]` — min-heap indexed by Entry slot position
  - O(log n) push/update/remove
  - Slot → heap position backpointer for O(1) lookup
- `touchCount_` + `rebuildThreshold_` — periodic full heap rebuild from stale atomic updates

### Two scoring atoms
1. **Desire** (recency proxy): +2 on Set (insert), +2 on Find (hit), +1 on Set (update). Decays /2 per Decay() call.
2. **Frequency** (popularity): +1 on every access. Slowly grows (+f/8 per Decay()). Capped at 31.

### Lock-free reads
- `Find()`: no SpinLock. Atomic reads/writes on desire/freq/score. Heap update deferred to periodic rebuild (every capacity/8 touches or Decay() call).
- `Set()`: SpinLock required (eviction + heap modification).
- `Erase/EraseByHash`: SpinLock required (backward-shift deletion in desire map).

### Eviction flow
On Set failure: pop min-score from heap → FindAndEraseByHash → fire eviction callback → retry Set.
Stale heap entries (already evicted by callback or page management) are skipped.

## Benchmark Results (2026-04-28, VPS, single NUMA node)

### Throughput (IsolatedBench, 10k ops × 5 iters, 64B values, no pressure)

| Policy | Set p50 | Set ops/s | Get p50 | Get p99 | Get ops/s |
|--------|---------|-----------|---------|---------|-----------|
| ArcPolicy | 247ns | 3.28M/s | **132ns** | 3500ns | 5.08M/s |
| StandardRemarc | 291ns | 3.13M/s | 205ns | 3400ns | 3.09M/s |
| AugAdaptPolicy | 639ns | 5.51M/s | 419ns | 3250ns | 6.41M/s |
| **NativeRemarc** | **186ns** | **4.96M/s** | 137ns | **267ns** | 6.06M/s |

Key observations:
- NativeRemarc Set p50 is **best** (186ns) — lock-free Find means less lock contention overall
- NativeRemarc Get p99 is **12× better** than ARC (267ns vs 3500ns)
- NativeRemarc Get p50 matches ARC (137ns vs 132ns)
- StandardRemarc and AugAdapt carry ManagePages overhead (SIMD scan: 87-114μs per pass)

### Hit Rate (128 pages, 16k key capacity, 20k universe, Zipf 0.99, 100k ops)

| Policy | Hit Rate | vs ARC |
|--------|----------|--------|
| **ArcPolicy** | **84.0%** | baseline |
| StandardRemarc | 71.4% | -12.6 pp |
| AugAdaptPolicy | 71.4% | -12.6 pp |
| **NativeRemarc** | **84.0%** | **0.0 pp** |

StandardRemarc/AugAdapt lose 12.6pp because ManagePages evicts pages that ARC's internal logic wouldn't evict.
NativeRemarc matches ARC exactly because it owns eviction directly — no conflicting page management.

### Scan Resistance (64 pages, 30k cold scan keys, 5k hot Zipf keys)

| Policy | Warmup | Recovery | vs ARC |
|--------|--------|----------|--------|
| ArcPolicy | 87.2% | 95.8% | baseline |
| NativeRemarc | 87.2% | 95.8% | 0.0 pp |

Identical across all page counts (32, 48, 64, 96, 128).
The frequency atom doesn't differentiate because page-level capacity (not KeyStore capacity) is the bottleneck.
Both caches evict ALL hot keys when 30k cold keys flood in, regardless of scoring.

## Key Insights

### 1. Advisory mode ceiling (Finding: advisory-mode-ceiling)
All REMARC variants that sit on top of ARC (StandardRemarc, AugAdapt) lose hit rate because
ManagePages evicts pages independently from ARC's internal T1/T2 eviction. ARC optimizes
eviction order at the key level; ManagePages evicts at the page level. These conflict.

NativeRemarc, by owning the cache directly, eliminates this conflict. Hit rate matches ARC.
This validates the advisory vs native mode distinction in the REMARC framework.

### 2. Lock-free reads + deferred heap = best latency profile
The deferred heap update strategy (atomic writes on Find, periodic rebuild) achieves:
- Same Get p50 as ARC (lock-free CAS path)
- 12× better Get p99 (no ARC list traversal stalls)
- Best Set p50 (no contention from reads)

### 3. Page capacity, not KeyStore capacity, determines hit rate
In FurrBall, page slots (4KB page / 64B value ≈ 50 keys/page) are the scarce resource.
KeyStore capacity is 128× page capacity. Pages always fill first.
Both ARC and NativeRemarc see the same effective capacity, producing identical hit rates.
NativeRemarc's scoring advantage only manifests when KeyStore IS the bottleneck, or when
page management uses the store's desire (see Future Work).

### 4. Frequency atom is latent (not yet exploitable)
The `(desire << 4) | freq` composite scoring doesn't beat ARC on Zipf workloads because:
- Under uniform pressure (scan floods), every hot key gets evicted regardless of score
- Under Zipf pressure, desire alone provides sufficient discrimination
- Frequency needs a scenario where recency changes but popularity persists (e.g., mixed
  temporal workload with intermittent access to popular keys)

### 5. StandardRemarc/AugAdapt ManagePages penalty is structural, not tunable
The -12.6pp penalty comes from ManagePages evicting pages that ARC's LRU wouldn't evict.
This cannot be fixed by tuning ThetaEvict or migration budget — the fundamental conflict
between page-level eviction and key-level ARC eviction persists.

## Architecture Gap: ManagePages Cannot Query Store's Desire

ManagePages builds coldMask from `page.TempCtrl[idx]` → `Policy::EvictScore(tc)`.
For NativeRemarcPolicy, `HasPerKeyState=false`, `InitialState()=0` → TempCtrl is always 0.
The desire map lives inside NativeRemarcStore (private). No bridge exists.

### Proposed bridge
Add `GetDesire(uint64_t h2) const` to the store interface.
ManagePages queries `details->KeyStore.GetDesire(hp.h2)` instead of `Policy::EvictScore(page.TempCtrl[idx])`.
This gives NativeRemarc store-driven page management:
- Desire map (starts at 0 = cold, earns warmth) provides better discrimination than TempCtrl (starts at 15 = max warmth)
- Temporal cohesion: keys that went cold together have low desire → consolidated onto cold pages → evicted as a unit
- Reduced I/O: page-level eviction instead of per-key eviction

### Interface change
```cpp
// In CMap.h ConcurrentARC:
uint8_t GetDesire(uint64_t) const { return 0; } // ARC has no desire

// In NativeStore.h NativeRemarcStore:
uint8_t GetDesire(uint64_t h2) const {
    size_t idx = probe(h2);
    return (idx == SIZE_MAX || !slots_[idx].occupied) ? 0 : slots_[idx].desire.load(relaxed);
}

// In Furrballs.cpp ManagePages:
if constexpr (Policy::HasDesire) {
    // Use store desire instead of Policy::EvictScore
    uint8_t d = details->KeyStore.GetDesire(page.KeyIndex[idx].h2);
    bool cold = (d < threshold);
    coldMask |= (cold << idx);
}
```

## Future Work
1. Bridge ManagePages to store's desire → enable page management for NativeRemarc
2. Test with I/O: RocksDB write-back on page eviction. Measure write amplification.
3. Multi-node: NativeRemarc with cross-node desire (S_i(K) per node)
4. Workload that exploits frequency: mixed temporal with intermittent popular-key access
5. Adaptive scoring weights: dynamically adjust desire vs freq based on CV of access pattern
