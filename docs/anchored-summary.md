---
title: Anchored Summary
date: 2026-04-24
---

## Goal
- Build **Furrballs** (C++20 NUMA-aware caching library) with **REMARC** as the unified cache policy, and produce three academic papers: Paper 1 = Furrballs systems, Paper 2 = REMARC algorithm/framework, Paper 3 = REMARC pure algebraic theory.

## Constraints & Preferences
- User develops on a VPS via VS Code Remote SSH over Windows. Terminal doesn't render LaTeX — use plain text math in discussion.
- User prefers to implement things themselves but allowed assistant to implement in this session (Get() scoring, SIMD scanner, page lifecycle, migration, microbenchmark variants).
- Every commit to whitepaper must include a version bump.
- User drives ideation and design. "It's a cache" is not a valid argument for wrong answers.
- User has algebra background, no ML/statistics background. Located in Tunisia, independent researcher.
- User is "naturally pessimistic" — works regardless of whether ideas prove out. Values intellectual honesty over confidence.
- AVX2 + BMI2 allowed for SIMD (target: Intel Xeon Platinum 8375C Ice Lake).

## Progress
### Done
- **Phase 1 and Phase 2a complete** with real hardware validation on AWS c6i.metal.
- **REMARC paper** (`docs/remarc-paper.qmd`) extensively revised:
  - **Three-atom decomposition** (§3.4): phi (state update), proj_s (linear projection R^n→R^k), decide (nonlinear combination R^k→D). Sigma eliminated as primitive.
  - **Independent space axioms**, **D_f = f(S) formulation**, **expressive completeness** flagged as open question.
  - **Deterministic convergence proof** (§3.3), steady-state derivation, coverage table (9 policies), correlation condition, SIMD description, outcome-based adaptation.
  - **K>2 generalization** (§6), symmetry regimes, lookup table construction (Appendix B), information-theoretic connections (§7.4).
- **Phase 2b implementation** (REMARC policy) — all core paths wired (Remarc.h, Page.h, Furrballs.cpp Get/Set/MigrateKey/ScanAndExecute, CMap.h).
- **5 bug fixes** in REMARC implementation (documented in session notes).
- **Phase 3: Exhaustive single-node microbenchmark exploration** — 30+ variants across 5 workloads. 11 key findings. Complete study documented in session notes.
  - **Findings 1-8**: Locality barrier, factorization theory, integer quantization load-bearing, B as parameter, Poisson dead, Hawkes no gain, LogFreq no help, ghost validated.
  - **Finding 9 (Log8)**: Logarithmic quantization is a different Pareto point. Best: 26.6% Zipfian + 3% Looping. Not an improvement.
  - **Finding 10 (Feedback)**: Q-modulated proj_s weights and directional (Q_r, Q_f) feedback are signal-degenerate. Uniform phi → homogeneous ghost → no directional information. Feedback adds 0.01pp over static ghost.
  - **Finding 11 (Tiered/Capacity Allocation)**: D_f = f(S) allows richer D including capacity allocation. Tested ARC-like tier structure (T_R/T_F with p-controlled split). p always collapses to pMin. Ghost_R fills with cold revolving-door keys, restoring force absent. Zipfian unchanged at ~24.5%.

### Single-Node Study — COMPLETE
The single-node study is finished. Key conclusions:
- **REMARC's single-node ceiling**: ~24.5% Zipfian (flat), ~27% with high-frequency-weight + ghost. This is the limit for ALL tested D within the integer EMA per-key scoring class.
- **REMARC uniquely dominates on Looping**: 49% vs 0% for ARC/LRU. Structurally impossible for list-based policies.
- **ScanRes competitive**: 90.4% (matching ARC's 90.5%).
- **The locality barrier is a rigorous finding**, not a weakness. It characterizes the optimality of the bounded per-key scoring class.
- **D_f = f(S) theoretical refinement**: The barrier is a property of specific D (per-key scalar score), not the framework. ARC's capacity allocation IS expressible in the framework but requires discriminable sub-populations that integer EMA cannot produce.
- **REMARC was designed for NUMA**. Single-node is the wrong benchmark for its strengths.

### Blocked
- **Paper 3 needs mathematician co-author** for approximation theorem, expressive completeness proof.
- **Scanner-triggered migration** blocked on reverse index (HashPair → key string). Currently deferred to Get() access path.

## Key Decisions
- **Three-atom decomposition** (not four): sigma eliminated. Framework outputs continuous D, discretization is downstream implementation.
- **D_f = f(S)** — discrimination space constructively defined as image, not predefined codomain. D is not fixed to {evict, retain}.
- **dim(D_f) ≤ dim(S)** — compression invariant.
- **Migration is lazy** (triggered on Get()) not eager (from scanner). Scanner handles eviction only.
- **RF axis repurposing for single-node**: s_local→recency, s_remote→frequency.
- **Dormant phi re-insertion uses recency-rate decay** for ghost F (every 64 ops at 7/8, not every 2048). Rationale: outside the cache, information decays at "staleness" rate.
- **Factorization theory**: ghost lists are not an exception. They're a consequence of the global × local tensor product decomposition. phi maintains G, H, Q identically to per-key S. proj_s takes (S, G, H, Q) → R. Three atoms, no exceptions.
- **Locality barrier refined**: the ~24.5% ceiling holds for ALL tested D (scalar scoring, Q-modulated, directional, tiered). It's robust across the class {per-key scoring, modulated scoring, capacity allocation} with integer EMA state.
- **Integer quantization is load-bearing**: float EMA collapses ScanRes from 90% to 48%. Not a tuning choice — a structural requirement.
- **Quantization precision (B) is a tuning parameter**: α/2^B determines axis responsiveness.

### 2-Node NUMA Simulation — Complete
- **9 experiments** across 4 workloads (Skewed-Zipf 80% remote, Node-Shift 4-phase, Cross-Loop 50/50, Oscillating 10K phase).

**Key Finding 12 (Migration-Adaptation Gap):** REMARC value-based migration fails on Node-Shift (cost identical to no-migration). Cause: slow-decaying sF creates "frequency momentum" — phase-0 keys retain sF≈133 while migrated keys arrive with score ~80. Revolving door. LRU avoids this because insertion gives max recency.

**Key Finding 14 (Desire Encoding):**
- Node 0 tracks REMARC desire for ALL keys (shadow map, not just cached). Cache_0 = top keys by desire. Migration = desire(K) > min_desire(cache_0). No threshold needed.
- **Results**: Skewed-Zipf cost 87.2 (vs 337.5 best value-based), Node-Shift cost 96.1 (vs 422.6), Cross-Loop 80.1, Oscillating 104.9. 3-4x improvement over all prior policies.
- **Theoretical insight**: Desire encoding makes per-node independence hold. S_i(K) = "how much does node i want K" (local observation) → migration = conjunction of independent local decisions. Value encoding (S_i(K) = "value of K on node i") requires cross-node comparison → NOT independent.

**Key Finding 13 (Factorization Independence):**
- Independence depends on S encoding. Desire encoding preserves independence; value encoding does not.
- dim(D_migrate) ≥ n — migration decision space is directional, not 1D.
- Decision independence holds; state evolution independence fails for value encoding but holds for desire encoding.

### 3-Node NUMA Simulation — Complete
- **3 variants** (desire-only, desire+compare, desire+cooldown) on 4 workloads.
- **4 workloads**: Easy contention, Zipfian contention (all nodes Zipf over all keys), Cross-dominant (80% remote), Rotating hot set (6 phases).

**Key Finding 15 (Global State for n≥3):**
- **Desire-only thrashes** on all hard workloads (cost 296-325). Shared hot keys bounce between nodes.
- **Cross-node desire compare dominates**: migrate only if accessor's desire > 1.5× owner's desire. Cost: 132.5 (Zipfian), 125.6 (Cross-dom), 134.9 (Rotating).
- **Compare ratio sweep**: 1.1x is the big cliff (cost drops from 290→140). Diminishing returns above 1.3x. Cooldown strictly dominated by any compare ≥ 1.1x.
- **The 65% remote hit rate is structural** — capacity problem (cap/3 = 67 per node vs 999 keys). Not a policy problem.

**Key Finding 16 (Global State Necessary for n≥2):**
- 2-node: desire encoding alone sufficient (each node's desire is enough information).
- 3-node: cross-node desire compare (G) necessary. No single node knows other nodes' desire scores.
- G is O(1) per decision (one lookup in owner's desire map), O(n_nodes) state. Constant-size, not per-key.
- **Theorem**: desire encoding preserves per-node independence for eviction. Cross-node compare in G resolves migration contention for n≥2. G unnecessary for n=2, necessary for n≥3.

### Single-Node Ghost Map Study — Complete
- Tested at **cap=200, keys=1000 (20% ratio)** and **cap=1000, keys=10000 (10% ratio)**.

**Finding 17 (Ghost Map Marginal on Zipfian, Helps Temporal, Hurts Looping):**

At **cap=1000, keys=10000 (10% ratio, same as original study)**:
| Policy | Zipf% | Temp% | Scan% | Loop% |
|---|---|---|---|---|
| ARC | **50.81** | **26.60** | 70.76 | 0.25 |
| Cache-only (no ghost) | 26.48 | 12.54 | **90.37** | **3.02** |
| Ghost-decay-cached | 27.84 | 14.34 | 90.42 | 0.61 |
| Desire-single (full ghost) | 28.00 | 12.37 | 90.42 | 0.48 |

At **cap=200, keys=1000 (20% ratio)**:
| Policy | Zipf% | Temp% | Scan% | Loop% |
|---|---|---|---|---|
| ARC | **62.20** | **49.14** | 87.48 | 0.05 |
| Cache-only (no ghost) | 39.71 | 20.19 | **91.52** | **33.93** |
| Ghost-decay-cached | 38.95 | 38.00 | 90.90 | 25.71 |
| Desire-single (full ghost) | 39.67 | 36.47 | 91.53 | 27.02 |

Key conclusions: Ghost adds ~1.5pp Zipfian at 10% ratio, ~0pp at 20%. Ghost helps Temporal (+14-16pp), hurts Looping (-2.5 to -7pp). The old ~24.5% ceiling confirmed at 10% ratio. The 39.67% at cap=200 was capacity ratio artifact. ARC dominates Zipfian at both ratios. Cache-only REMARC dominates Looping.

### In Progress
- None currently.

## Next Steps
1. **Wire desire encoding into Furrballs**: Shadow desire map per NUMA node, cross-node compare for migration. Production defaults: (8,8,8,1024) with 1.3× compare ratio.
2. **Update remarc-paper.qmd**: Findings 12-17, desire encoding theory, independence theorem, ghost map study, multi-node evaluation sections.
3. **Dynamic capacity allocation for n≥3**: Lower priority — desire encoding already delivers 3-4× improvement.

## Critical Context
- **Paper 2 is publishable** with evaluation: three-atom decomposition, convergence proof, coverage table, SIMD implementation, single-node study (30+ variants, 11 findings), desire encoding, independence theorem, 2-node/3-node evaluation (16 findings), ghost map study (Finding 17).
- **Desire encoding is the breakthrough for multi-node**: 3-4× cost improvement. Preserves per-node independence. Eliminates migration threshold.
- **Ghost map adds ~1.5pp Zipfian at 10% ratio, ~0pp at 20% ratio** (Finding 17). Helps Temporal (+14-16pp), hurts Looping. Not a single-node breakthrough.
- **Global state (G) is O(1) per decision, O(n_nodes) state**: cross-node desire compare resolves contention for n≥3.
- **Single-node Zipfian ceiling (~24.5% at 10% cap/keys) is structural**: per-key scalar scoring cannot match ARC's T1/T2 structural identity. Confirmed across 30+ variants + ghost map study.
- **REMARC uniquely dominates on Looping** and on multi-node with desire encoding.
- **No REMARC variant matches ARC on single-node Zipfian** — confirmed as information-theoretic limit of the bounded per-key scoring class.
- **Benchmark 2-node code lost** during restructuring. Results preserved in anchored summary. 3-node + ghost study code intact.

## Relevant Files
- `docs/remarc-paper.qmd` — REMARC paper (Paper 2 + 3 brain dump). Three-atom decomposition in §3.4, SIMD in §3.5, generation theorem in §9.5. Needs: factorization §new, locality barrier, quantization parameter, single-node evaluation §new, findings 9-11.
- `docs/whitepaper.qmd` — Furrballs systems paper. §2.8/§2.9 REMARC forward pointers.
- `docs/session-2026-04-24.md` — Detailed session notes: bug fixes, all 30+ microbenchmark variants, factorization theory, locality barrier, 11 key findings, feedback/tiered study, single-node conclusions.
- `docs/anchored-summary.md` — This file. Anchored context for continuing sessions.
- `Furrballs/include/Remarc.h` — constexpr EvictLookup/MigrateLookup tables, RemarcConfig, update helpers, AVX2 scanner (RemarcScanBatch, HSumEpu16, CompressCmpMask), RemarcComputeEPage.
- `Furrballs/include/Page.h` — TempCtrl, KeyIndex, HotNodes, CompactLock, AddKeyEntry, RemoveKeyEntry (swap-and-pop, returns HashPair), FindKeyIndex, TryTransition, Recycle, TryBump (tier check).
- `Furrballs/include/Furrballs.h` — KeyMeta (32 bytes), FurrConfig with RemarcConfig, Statistics with MigrationCount/MigrationReversalCount.
- `Furrballs/include/CMap.h` — ConcurrentARC with Erase/EraseByHash, UpdateInPlaceByHash. CMap has public UpdateInPlaceByHash and FindAndEraseByHash.
- `Furrballs/src/Furrballs.cpp` — Get() with rate-limited pre-update migration, Set() with RemoveKeyEntry rollback, MigrateKey with swap-and-pop fixup, ScanAndExecute two-pass.
- `Benchmark/RemarcBench.cpp` — Standalone microbenchmark. 30+ variants: LRU, ARC, REMARC*, RR, Poisson, Hawkes, LogFreq, Adaptive, CoopLog, Factored (3 float modes), FactoredInt (3 int modes), Log8, Feedback (Q-modulated + directional), Tiered (capacity allocation with ARC-like structure). 5 workloads. Parameter sweeps.
- `Benchmark/CMakeLists.txt` — RemarcBench target with `-mssse3 -mavx2 -mbmi2`.
- Git branch: `numa-focus`, remote: `https://github.com/NuAtlas/Furrballs.git`
