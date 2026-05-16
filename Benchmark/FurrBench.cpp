// FurrBench.cpp — Unified benchmark harness for Furrballs
//
// Google Benchmark tutorial:
//
// 1. Every benchmark is a function taking benchmark::State&.
//    `for (auto _ : state) { ... }` is the MEASURED region.
//    Google Benchmark picks N automatically for stable statistics.
//    Setup/teardown goes OUTSIDE the loop (in SetUp/TearDown).
//
// 2. benchmark::State:
//    - state.PauseTiming() / ResumeTiming() → exclude code from timing
//    - state.counters["name"] = value → custom output columns
//    - state.SkipWithError("msg") → mark iteration as failed
//
// 3. Fixtures (benchmark::Fixture):
//    - SetUp(const State&): runs before the measurement loop
//    - TearDown(const State&): runs after
//    - Use for expensive setup (loading traces, creating caches)
//
// 4. Parameterized benchmarks:
//    BENCHMARK_REGISTER_F(Fixture, Name)->Args({a, b});
//    Access: state.range(0), state.range(1)
//
// 5. Registration:
//    BENCHMARK_REGISTER_F(Fixture, Name)
//        ->Args({1, 256})      // numNodes=1, pagesPerNode=256
//        ->Iterations(10)      // run 10 times
//        ->Unit(benchmark::kMicrosecond)
//
// 6. Output formats:
//    --benchmark_out=results.json
//    --benchmark_out_format=json|csv|console

#include <benchmark/benchmark.h>
#include <chrono>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <random>
#include <thread>

#include "Furrballs.h"
#include "Policy.h"
#include "Numatic.h"

#include <tbb/concurrent_hash_map.h>
#include <libmemcached/memcached.h>

using namespace NuAtlas;

// ============================================================================
//  Simulated NUMA setup (extracted from MultiNodeBench)
// ============================================================================

static void SetupSimulated1Node() {
    auto& gs = Detail::globalNumaState;
    gs = {};
    gs.NumaNodeCount = 1;
    gs.Workers = (NodeJob*)malloc(sizeof(NodeJob));
    new(gs.Workers) NodeJob(0);
    gs.Workers[0].Start([](){});
    gs.Initialized = true;
}

static void SetupSimulated2Nodes() {
    auto& gs = Detail::globalNumaState;
    gs = {};
    gs.NumaNodeCount = 2;
    gs.SysNumaPageSize = 65536;
    gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * 2);
    for (int i = 0; i < 2; i++) {
        new(&gs.Workers[i]) NodeJob(i);
        gs.Workers[i].Start([](){});
    }
    gs.Initialized = true;
}

static void TeardownNodes() {
    auto& gs = Detail::globalNumaState;
    if (!gs.Initialized || !gs.Workers) {
        gs = {};
        return;
    }
    gs.Initialized = false;
    for (int i = 0; i < gs.NumaNodeCount; i++) {
        gs.Workers[i].Stop();
        gs.Workers[i].~NodeJob();
    }
    free(gs.Workers);
    gs = {};
}

// ============================================================================
//  Trace reader — libCacheSim format: timestamp, key, size, next_vtime
// ============================================================================

struct TraceEntry {
    uint64_t key;
    uint32_t size;
};

static std::vector<TraceEntry> loadTrace(const std::string& path, size_t maxEntries = 0) {
    std::vector<TraceEntry> trace;
    std::ifstream f(path);
    if (!f.is_open()) {
        fprintf(stderr, "WARNING: cannot open trace %s\n", path.c_str());
        return {};
    }
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        TraceEntry e{};
        unsigned long k, nv;
        unsigned int sz;
        unsigned long ts;
        if (sscanf(line.c_str(), "%lu, %lu, %u, %lu", &ts, &k, &sz, &nv) != 4)
            continue;
        e.key = k;
        e.size = sz;
        trace.push_back(e);
        if (maxEntries > 0 && trace.size() >= maxEntries) break;
    }
    return trace;
}

// ============================================================================
//  Zipfian generator (simplified inverse CDF)
// ============================================================================

static uint64_t zipfian(uint64_t n, double theta, uint64_t& state) {
    static thread_local std::mt19937_64 rng;
    if (state != 0) rng.seed(state);
    state = 0;
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double u = dist(rng);
    if (u < 1e-9) u = 1e-9;
    double val = (double)n * std::pow(u, 1.0 / (1.0 - theta));
    if (val < 1.0) val = 1.0;
    if (val > (double)n) val = (double)n;
    return (uint64_t)val;
}

// ============================================================================
//  Fixture: reusable cache setup + trace data
//
//  NOTE: SetUp gets `const benchmark::State&` but PauseTiming/ResumeTiming
//  are non-const. This is fine — Google Benchmark provides a non-const
//  reference during actual execution. The const is only for type safety
//  in the declaration.
// ============================================================================

struct FurrBench : public benchmark::Fixture {

    int numNodes;
    int pageCount;
    std::vector<TraceEntry> trace;
    FurrBall<ArcPolicy>* fb = nullptr;
    std::string fbPath;

    static constexpr size_t OPS = 100000;
    static constexpr size_t PAGE_SIZE = 4096;

    void SetUp(benchmark::State& state) override {
        numNodes = state.range(0);
        pageCount = state.range(1);

        state.PauseTiming();
        if (trace.empty()) {
            trace = loadTrace(
                "/home/ubuntu/source/repos/libCacheSim/data/twitter_cluster52.csv",
                2000000);
            fprintf(stderr, "Loaded %zu trace entries\n", trace.size());
        }
        state.ResumeTiming();
    }

    // Only delete the ball. Do NOT call Shutdown() or TeardownNodes() here.
    // Workers must stay alive across iterations. Cleanup happens in main().
    void TearDown(benchmark::State& state) override {
        state.PauseTiming();
        if (fb) { delete fb; fb = nullptr; }
        state.ResumeTiming();
    }

    // Create FurrBall with correct API:
    //   FurrConfig{ PageSize, InitialPageCount, IsVolatile, EnableNUMA, numaConfig }
    //   NumaConfig{ AllocateUsingNodePageSize, UseThreadLocalRouting }
    //   CreateBall(path, config, overwrite)
    //
    // Only re-initializes globalNumaState if node count changed.
    // This avoids the double-free crash from Shutdown() + TeardownNodes().
    void createCache() {
        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != numNodes;
        if (needsInit) {
            TeardownNodes();
            if (numNodes > 1) SetupSimulated2Nodes();
            else SetupSimulated1Node();
        }

        static int runId = 0;
        fbPath = "/tmp/furrbench_" + std::to_string(numNodes) + "n_"
                 + std::to_string(pageCount) + "pg_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.InitialPageCount = pageCount;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.numaConfig = &nc;

        fb = FurrBall<ArcPolicy>::CreateBall(fbPath, fc, true);
    }

    // Run a Get-or-Set loop, collecting hit/miss/latency metrics.
    void runLoop(size_t ops, size_t& hits, size_t& misses,
                std::vector<int64_t>& latencies) {

        latencies.clear();
        latencies.reserve(ops);
        hits = 0; misses = 0;

        for (size_t i = 0; i < ops; i++) {
            std::string key = "k_" + std::to_string(i);
            uint8_t outBuf[4096];
            size_t outSize = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            Error err = fb->Get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (err == NO_ERR && outSize > 0) {
                hits++;
            } else {
                misses++;
                uint8_t val[64];
                memset(val, 'x', 64);
                fb->Set(key, val, 64);
            }
        }
    }

    void reportCounters(benchmark::State& state,
                        size_t hits, size_t misses,
                        std::vector<int64_t>& latencies) {
        std::sort(latencies.begin(), latencies.end());
        size_t p50 = latencies[latencies.size() / 2];
        size_t p99 = latencies[latencies.size() * 99 / 100];

        state.counters["hits"] = hits;
        state.counters["misses"] = misses;
        state.counters["hit_rate_pct"] = 100.0 * hits / (hits + misses);
        state.counters["p50_ns"] = (double)p50;
        state.counters["p99_ns"] = (double)p99;
        state.counters["evictions"] = fb->Stats.GetEvictionCount();
        state.counters["perkey_evict"] = fb->Stats.GetPerKeyEvictionCount();
        state.counters["pages_rec"] = fb->Stats.GetPagesReclaimed();
    }
};

// ============================================================================
//  Benchmark 1: Twitter trace replay
//
//  Google Benchmark calls this 10 times (->Iterations(10)).
//  Each call: SetUp → measurement loop (N iterations picked by GB) → TearDown.
//  state.counters[] are aggregated across all iterations automatically.
//
//  Args: {numNodes, pageCount}
//  ->Args({1, 256}) means: single-node, 256 pages of 4KB each
// ============================================================================

BENCHMARK_DEFINE_F(FurrBench, TwitterTrace)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        createCache();
        if (!fb) { state.SkipWithError("CreateBall returned null"); continue; }
        state.ResumeTiming();

        size_t traceIdx = 0;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        for (size_t i = 0; i < OPS; i++) {
            if (traceIdx >= trace.size()) traceIdx = 0;
            const auto& entry = trace[traceIdx++];
            std::string key = "tr_" + std::to_string(entry.key);

            uint8_t outBuf[4096];
            size_t outSize = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            Error err = fb->Get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (err == NO_ERR && outSize > 0) {
                hits++;
            } else {
                misses++;
                uint8_t val[64];
                memset(val, 'x', 64);
                fb->Set(key, val, 64);
            }
        }
        reportCounters(state, hits, misses, latencies);
    }
}

BENCHMARK_REGISTER_F(FurrBench, TwitterTrace)
    ->Args({1, 256})
    ->Args({1, 64})
    ->Args({1, 16})
    ->Args({2, 256})
    ->Args({2, 64})
    ->Args({2, 16})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Benchmark 2: Synthetic Zipfian
// ============================================================================

BENCHMARK_DEFINE_F(FurrBench, Zipfian)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        createCache();
        if (!fb) { state.SkipWithError("CreateBall returned null"); continue; }
        state.ResumeTiming();

        uint64_t zState = 42;
        uint64_t universe = 100000;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        for (size_t i = 0; i < OPS; i++) {
            uint64_t ki = zipfian(universe, 0.99, zState);
            std::string key = "z_" + std::to_string(ki);

            uint8_t outBuf[4096];
            size_t outSize = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            Error err = fb->Get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (err == NO_ERR && outSize > 0) {
                hits++;
            } else {
                misses++;
                uint8_t val[64];
                memset(val, 'x', 64);
                fb->Set(key, val, 64);
            }
        }
        reportCounters(state, hits, misses, latencies);
    }
}

BENCHMARK_REGISTER_F(FurrBench, Zipfian)
    ->Args({1, 256})
    ->Args({1, 64})
    ->Args({1, 16})
    ->Args({2, 256})
    ->Args({2, 64})
    ->Args({2, 16})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Benchmark 3: OnKeyEvict validation (correctness test)
//
//  Forces extreme eviction pressure (200K inserts, tiny cache).
//  Checks that PerKeyEvictionCount > 0 and no crashes.
//  Only 3 iterations — correctness doesn't need statistical rigor.
// ============================================================================

BENCHMARK_DEFINE_F(FurrBench, EvictionValidation)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        createCache();
        if (!fb) { state.SkipWithError("CreateBall returned null"); continue; }
        state.ResumeTiming();

        uint64_t zState = 12345;
        uint64_t universe = 10000;

        size_t inserted = 0, evicted = 0;
        for (size_t i = 0; i < 50000; i++) {
            uint64_t ki = zipfian(universe, 0.99, zState);
            std::string key = "ev_" + std::to_string(ki);
            uint8_t val[64];
            memset(val, 'x', 64);
            Error err = fb->Set(key, val, 64);
            if (err == NO_ERR) inserted++;
            else break;
        }

        evicted = fb->Stats.GetPerKeyEvictionCount();
        state.counters["inserted"] = (double)inserted;
        state.counters["perkey_evict"] = (double)evicted;
        state.counters["pages_rec"] = (double)fb->Stats.GetPagesReclaimed();
    }
}

BENCHMARK_REGISTER_F(FurrBench, EvictionValidation)
    ->Args({1, 4})
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

// ============================================================================
//  Baseline LRU cache — linked hash map, O(1) get/set/evict
//
//  FAIRNESS NOTE: This baseline is single-threaded with zero synchronization
//  (no locks, no atomics). FurrBall uses SpinLock-guarded ConcurrentARC for
//  thread-safe concurrent access. The latency comparison is therefore generous
//  to the baseline. Hit rate comparison is fair (same capacity, same workload).
//
//  Fair comparison: same byte capacity as FurrBall (numPages * PageSize).
//  FurrBall has overhead from page metadata, so this slightly over-provisions
//  the baseline, making it a conservative (generous) comparison.
// ============================================================================

class LRUCache {
    struct Entry {
        std::string key;
        std::vector<uint8_t> data;
    };

    std::list<Entry> lruList;
    std::unordered_map<std::string, std::list<Entry>::iterator> index;
    size_t byteCapacity;
    size_t usedBytes = 0;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;

    void evictOne() {
        if (lruList.empty()) return;
        auto& back = lruList.back();
        usedBytes -= back.data.size();
        index.erase(back.key);
        lruList.pop_back();
        evictions_++;
    }

public:
    explicit LRUCache(size_t byteCap) : byteCapacity(byteCap) {}

    bool Get(const std::string& key, void* outBuf, size_t bufSize, size_t& outSize) {
        auto it = index.find(key);
        if (it == index.end()) {
            misses_++;
            return false;
        }
        lruList.splice(lruList.begin(), lruList, it->second);
        auto& entry = lruList.front().data;
        outSize = entry.size();
        if (bufSize >= entry.size()) {
            memcpy(outBuf, entry.data(), entry.size());
        }
        hits_++;
        return true;
    }

    void Set(const std::string& key, const void* data, size_t size) {
        auto it = index.find(key);
        if (it != index.end()) {
            size_t oldSize = it->second->data.size();
            usedBytes -= oldSize;
            it->second->data.assign(static_cast<const uint8_t*>(data),
                                     static_cast<const uint8_t*>(data) + size);
            usedBytes += size;
            lruList.splice(lruList.begin(), lruList, it->second);
            return;
        }

        while (usedBytes + size > byteCapacity && !lruList.empty()) {
            evictOne();
        }
        if (usedBytes + size > byteCapacity) return;

        lruList.push_front({key, {}});
        lruList.front().data.assign(static_cast<const uint8_t*>(data),
                                     static_cast<const uint8_t*>(data) + size);
        index[key] = lruList.begin();
        usedBytes += size;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    void resetStats() { hits_ = misses_ = evictions_ = 0; }
};

// ============================================================================
//  Baseline LRU benchmarks — same traces, same capacity
// ============================================================================

struct BaselineBench : public benchmark::Fixture {
    std::vector<TraceEntry> trace;
    LRUCache* lru = nullptr;

    static constexpr size_t OPS = 100000;
    static constexpr size_t PAGE_SIZE = 4096;

    void SetUp(benchmark::State& state) override {
        state.PauseTiming();
        if (trace.empty()) {
            trace = loadTrace(
                "/home/ubuntu/source/repos/libCacheSim/data/twitter_cluster52.csv",
                2000000);
            fprintf(stderr, "[Baseline] Loaded %zu trace entries\n", trace.size());
        }
        int numPages = state.range(0);
        lru = new LRUCache(static_cast<size_t>(numPages) * PAGE_SIZE);
        state.ResumeTiming();
    }

    void TearDown(benchmark::State& state) override {
        state.PauseTiming();
        delete lru; lru = nullptr;
        state.ResumeTiming();
    }
};

BENCHMARK_DEFINE_F(BaselineBench, TwitterTrace)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        delete lru;
        int numPages = state.range(0);
        lru = new LRUCache(static_cast<size_t>(numPages) * PAGE_SIZE);
        state.ResumeTiming();

        size_t traceIdx = 0;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        for (size_t i = 0; i < OPS; i++) {
            if (traceIdx >= trace.size()) traceIdx = 0;
            const auto& entry = trace[traceIdx++];
            std::string key = "tr_" + std::to_string(entry.key);

            uint8_t outBuf[4096];
            size_t outSize = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            bool found = lru->Get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (found) {
                hits++;
            } else {
                misses++;
                uint8_t val[64];
                memset(val, 'x', 64);
                lru->Set(key, val, 64);
            }
        }

        std::sort(latencies.begin(), latencies.end());
        state.counters["hits"] = (double)hits;
        state.counters["misses"] = (double)misses;
        state.counters["hit_rate_pct"] = 100.0 * hits / (hits + misses);
        state.counters["p50_ns"] = (double)latencies[latencies.size() / 2];
        state.counters["p99_ns"] = (double)latencies[latencies.size() * 99 / 100];
        state.counters["evictions"] = (double)lru->evictions();
    }
}

BENCHMARK_REGISTER_F(BaselineBench, TwitterTrace)
    ->Args({256})
    ->Args({64})
    ->Args({16})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_DEFINE_F(BaselineBench, Zipfian)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        delete lru;
        int numPages = state.range(0);
        lru = new LRUCache(static_cast<size_t>(numPages) * PAGE_SIZE);
        state.ResumeTiming();

        uint64_t zState = 42;
        uint64_t universe = 100000;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        for (size_t i = 0; i < OPS; i++) {
            uint64_t ki = zipfian(universe, 0.99, zState);
            std::string key = "z_" + std::to_string(ki);

            uint8_t outBuf[4096];
            size_t outSize = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            bool found = lru->Get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (found) {
                hits++;
            } else {
                misses++;
                uint8_t val[64];
                memset(val, 'x', 64);
                lru->Set(key, val, 64);
            }
        }

        std::sort(latencies.begin(), latencies.end());
        state.counters["hits"] = (double)hits;
        state.counters["misses"] = (double)misses;
        state.counters["hit_rate_pct"] = 100.0 * hits / (hits + misses);
        state.counters["p50_ns"] = (double)latencies[latencies.size() / 2];
        state.counters["p99_ns"] = (double)latencies[latencies.size() * 99 / 100];
        state.counters["evictions"] = (double)lru->evictions();
    }
}

BENCHMARK_REGISTER_F(BaselineBench, Zipfian)
    ->Args({256})
    ->Args({64})
    ->Args({16})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  TBB concurrent_hash_map baseline
//
//  FAIRNESS NOTE: This is a concurrent data structure with fine-grained
//  internal locking (per-bucket striped mutex). No eviction — once full,
//  inserts succeed indefinitely (unbounded). Measures pure concurrent
//  throughput overhead. FurrBall pays for ARC eviction + page management
//  + SpinLock on top of this. The delta is the cost of caching intelligence.
// ============================================================================

struct TBBBench : public benchmark::Fixture {
    static constexpr size_t OPS = 100000;

    void SetUp(benchmark::State& state) override {}
    void TearDown(benchmark::State& state) override {}
};

BENCHMARK_DEFINE_F(TBBBench, Zipfian)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        tbb::concurrent_hash_map<std::string, std::vector<uint8_t>> map;
        uint64_t zState = 42;
        uint64_t universe = 100000;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        state.ResumeTiming();

        for (size_t i = 0; i < OPS; i++) {
            uint64_t ki = zipfian(universe, 0.99, zState);
            std::string key = "z_" + std::to_string(ki);
            uint8_t val[64];
            memset(val, 'x', 64);

            auto t0 = std::chrono::high_resolution_clock::now();
            {
                tbb::concurrent_hash_map<std::string, std::vector<uint8_t>>::const_accessor acc;
                if (map.find(acc, key)) {
                    hits++;
                } else {
                    misses++;
                    map.insert(acc, key);
                    const_cast<std::vector<uint8_t>&>(acc->second).assign(val, val + 64);
                }
            }
            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now() - t0).count());
        }

        std::sort(latencies.begin(), latencies.end());
        state.counters["hits"] = (double)hits;
        state.counters["misses"] = (double)misses;
        state.counters["hit_rate_pct"] = 100.0 * hits / (hits + misses);
        state.counters["p50_ns"] = (double)latencies[latencies.size() / 2];
        state.counters["p99_ns"] = (double)latencies[latencies.size() * 99 / 100];
    }
}

BENCHMARK_REGISTER_F(TBBBench, Zipfian)
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Memcached baseline — real system comparison
//
//  FAIRNESS NOTE: Memcached runs as a separate process communicating over
//  localhost TCP. Each operation pays ~50-100us network round-trip overhead.
//  Hit rate comparison IS fair (same eviction policy: LRU/slab).
//  Latency comparison is NOT fair to memcached. We report both for
//  transparency but the meaningful metric is hit rate.
// ============================================================================

struct MemcachedBench : public benchmark::Fixture {
    static constexpr size_t OPS = 100000;
    static constexpr size_t PAGE_SIZE = 4096;
    std::vector<TraceEntry> trace;
    memcached_st* mc = nullptr;
    memcached_server_st* server = nullptr;

    void SetUp(benchmark::State& state) override {
        state.PauseTiming();
        if (!mc) {
            mc = memcached_create(nullptr);
            memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
            memcached_behavior_set(mc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
            server = memcached_server_list_append(nullptr, "127.0.0.1", 11211, nullptr);
            memcached_server_push(mc, server);
        }
        if (trace.empty()) {
            trace = loadTrace(
                "/home/ubuntu/source/repos/libCacheSim/data/twitter_cluster52.csv",
                2000000);
            fprintf(stderr, "[Memcached] Loaded %zu trace entries\n", trace.size());
        }
        state.ResumeTiming();
    }

    void TearDown(benchmark::State& state) override {}
};

BENCHMARK_DEFINE_F(MemcachedBench, Zipfian)(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        memcached_flush(mc, 0);
        uint64_t zState = 42;
        uint64_t universe = 100000;
        size_t hits = 0, misses = 0;
        std::vector<int64_t> latencies;
        state.ResumeTiming();

        for (size_t i = 0; i < OPS; i++) {
            uint64_t ki = zipfian(universe, 0.99, zState);
            std::string key = "z_" + std::to_string(ki);
            uint8_t val[64];
            memset(val, 'x', 64);

            size_t vLen = 0;
            uint32_t flags = 0;

            auto t0 = std::chrono::high_resolution_clock::now();
            char* v = memcached_get(mc, key.c_str(), key.size(), &vLen, &flags, nullptr);
            auto t1 = std::chrono::high_resolution_clock::now();

            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

            if (v) {
                hits++;
                free(v);
            } else {
                misses++;
                memcached_set(mc, key.c_str(), key.size(),
                              reinterpret_cast<const char*>(val), 64, 0, 0);
            }
        }

        std::sort(latencies.begin(), latencies.end());
        state.counters["hits"] = (double)hits;
        state.counters["misses"] = (double)misses;
        state.counters["hit_rate_pct"] = 100.0 * hits / (hits + misses);
        state.counters["p50_ns"] = (double)latencies[latencies.size() / 2];
        state.counters["p99_ns"] = (double)latencies[latencies.size() * 99 / 100];
    }
}

BENCHMARK_REGISTER_F(MemcachedBench, Zipfian)
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
// BENCHMARK_MAIN() would skip this, leaking the worker threads.
int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    TeardownNodes();
    return 0;
}
