// YCSBBench.cpp — YCSB (Yahoo! Cloud Serving Benchmark) workload for NUMA cache evaluation
//
// YCSB workloads:
//   A: 50% Read / 50% Update  (mixed read-write)
//   B: 95% Read / 5% Update   (read-heavy)
//   C: 100% Read              (read-only)
//   F: 50% Read / 50% RMW     (read-modify-write)
//
// Systems: FurrBallTL, FurrBallSN, CacheLib, RocksDB, TBB
//
// Args encoding:
//   state.range(0) = numThreads
//   state.range(1) = totalCapacityKB
//   state.range(2) = YCSB workload (10=A, 11=B, 12=C, 13=F)
//   state.range(3) = value size in bytes (64, 256, 1024)
//   state.range(4) = record count (key universe)

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <cmath>
#include <numeric>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <atomic>
#include <random>

#include "Furrballs.h"
#include "Policy.h"
#include "Numatic.h"

#include <tbb/concurrent_hash_map.h>

#include <rocksdb/cache.h>
#include <rocksdb/advanced_cache.h>

using namespace NuAtlas;

// ============================================================================
//  Shared utilities (from NUMABench)
// ============================================================================

struct ThreadResult {
    size_t hits = 0;
    size_t misses = 0;
    size_t gets = 0;
    size_t sets = 0;
    std::vector<int64_t> getLatencies;
    std::vector<int64_t> setLatencies;
    int numaNode = -1;
};

struct AggregateResult {
    size_t totalHits = 0;
    size_t totalMisses = 0;
    size_t totalGets = 0;
    size_t totalSets = 0;
    double hitRatePct = 0;
    double p50GetNs = 0;
    double p90GetNs = 0;
    double p99GetNs = 0;
    double p50SetNs = 0;
    double p90SetNs = 0;
    double p99SetNs = 0;
    double opsPerSec = 0;
    std::vector<double> perNodeP50Get;
};

static AggregateResult aggregate(const std::vector<ThreadResult>& results,
                                 double elapsedSec) {
    AggregateResult ar;
    std::vector<int64_t> allGet, allSet;

    size_t maxNode = 0;
    for (const auto& r : results) {
        ar.totalHits += r.hits;
        ar.totalMisses += r.misses;
        ar.totalGets += r.gets;
        ar.totalSets += r.sets;
        if (r.numaNode >= 0 && (size_t)r.numaNode > maxNode)
            maxNode = (size_t)r.numaNode;
    }

    ar.hitRatePct = 100.0 * ar.totalHits / (ar.totalHits + ar.totalMisses);
    ar.opsPerSec = (ar.totalGets + ar.totalSets) / elapsedSec;

    for (const auto& r : results) {
        allGet.insert(allGet.end(), r.getLatencies.begin(), r.getLatencies.end());
        allSet.insert(allSet.end(), r.setLatencies.begin(), r.setLatencies.end());
    }

    if (!allGet.empty()) {
        std::sort(allGet.begin(), allGet.end());
        ar.p50GetNs = (double)allGet[allGet.size() / 2];
        ar.p90GetNs = (double)allGet[allGet.size() * 90 / 100];
        ar.p99GetNs = (double)allGet[allGet.size() * 99 / 100];
    }
    if (!allSet.empty()) {
        std::sort(allSet.begin(), allSet.end());
        ar.p50SetNs = (double)allSet[allSet.size() / 2];
        ar.p90SetNs = (double)allSet[allSet.size() * 90 / 100];
        ar.p99SetNs = (double)allSet[allSet.size() * 99 / 100];
    }

    ar.perNodeP50Get.resize(maxNode + 1, 0);
    for (const auto& r : results) {
        if (r.numaNode >= 0 && !r.getLatencies.empty()) {
            auto lats = r.getLatencies;
            std::sort(lats.begin(), lats.end());
            ar.perNodeP50Get[r.numaNode] = (double)lats[lats.size() / 2];
        }
    }

    return ar;
}

static void reportAgg(benchmark::State& state, const AggregateResult& ar) {
    state.counters["hit_rate_pct"] = ar.hitRatePct;
    state.counters["p50_get_ns"] = ar.p50GetNs;
    state.counters["p90_get_ns"] = ar.p90GetNs;
    state.counters["p99_get_ns"] = ar.p99GetNs;
    state.counters["p50_set_ns"] = ar.p50SetNs;
    state.counters["p90_set_ns"] = ar.p90SetNs;
    state.counters["p99_set_ns"] = ar.p99SetNs;
    state.counters["ops_per_sec"] = ar.opsPerSec;
    for (size_t n = 0; n < ar.perNodeP50Get.size(); n++) {
        std::string p50name = "node" + std::to_string(n) + "_p50_get_ns";
        state.counters[p50name] = ar.perNodeP50Get[n];
    }
}

struct IterStats {
    double p50Get = 0, p90Get = 0, p99Get = 0;
    double p50Set = 0, p90Set = 0, p99Set = 0;
    double opsPerSec = 0;
    double hitRate = 0;
};

static double meanStd(const std::vector<double>& vals, double& out_std) {
    if (vals.empty()) { out_std = 0; return 0; }
    double sum = 0;
    for (double v : vals) sum += v;
    double m = sum / vals.size();
    double sq = 0;
    for (double v : vals) sq += (v - m) * (v - m);
    out_std = std::sqrt(sq / vals.size());
    return m;
}

static void reportAggStats(benchmark::State& state,
                            const std::vector<IterStats>& runs,
                            size_t numRuns) {
    std::vector<double> p50g, p90g, p99g, p50s, p90s, p99s, ops, hr;
    p50g.reserve(numRuns); p90g.reserve(numRuns); p99g.reserve(numRuns);
    p50s.reserve(numRuns); p90s.reserve(numRuns); p99s.reserve(numRuns);
    ops.reserve(numRuns); hr.reserve(numRuns);
    for (const auto& r : runs) {
        p50g.push_back(r.p50Get); p90g.push_back(r.p90Get); p99g.push_back(r.p99Get);
        p50s.push_back(r.p50Set); p90s.push_back(r.p90Set); p99s.push_back(r.p99Set);
        ops.push_back(r.opsPerSec); hr.push_back(r.hitRate);
    }
    double d; // discard std for individual reporting
    auto em = [&](const std::vector<double>& v, const char* name) {
        double m = meanStd(v, d);
        state.counters[name] = m;
        state.counters[std::string(name) + "_std"] = d;
    };
    em(p50g, "p50_get_ns"); em(p90g, "p90_get_ns"); em(p99g, "p99_get_ns");
    em(p50s, "p50_set_ns"); em(p90s, "p90_set_ns"); em(p99s, "p99_set_ns");
    em(ops, "ops_per_sec");
    double m = meanStd(hr, d);
    state.counters["hit_rate_pct"] = m;
}

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
//  YCSB workload types
// ============================================================================

enum class YCSBWorkload { A = 10, B = 11, C = 12, F = 13 };

struct YCSBOp {
    uint64_t keyIndex;
    bool isRead;
};

static double ycsbReadRatio(YCSBWorkload w) {
    switch (w) {
    case YCSBWorkload::A: return 0.50;
    case YCSBWorkload::B: return 0.95;
    case YCSBWorkload::C: return 1.00;
    case YCSBWorkload::F: return 0.50;
    }
    return 0.5;
}

static std::vector<YCSBOp> generateYCSBWorkload(YCSBWorkload wl, size_t opsPerThread,
                                                 uint64_t recordCount, int threadIndex,
                                                 int totalThreads) {
    std::vector<YCSBOp> ops;
    ops.reserve(opsPerThread);
    uint64_t zState = 42 + threadIndex * 1000;
    std::mt19937_64 rng(123 + threadIndex);
    double readRatio = ycsbReadRatio(wl);

    for (size_t i = 0; i < opsPerThread; i++) {
        uint64_t ki = zipfian(recordCount, 0.99, zState);
        bool isRead = (rng() % 10000) < (uint64_t)(readRatio * 10000);
        ops.push_back({ki, isRead});
    }
    return ops;
}

// ============================================================================
//  System adapters (same as NUMABench)
// ============================================================================

// --- FurrBall adapter (ThreadLocal or RoundRobin) ---
enum class Routing { ThreadLocal, RoundRobin };

template <Routing R, typename Policy = ArcPolicy>
struct FurrBallAdapter {
    static constexpr const char* Name = (R == Routing::ThreadLocal)
        ? "FurrBall_TL" : "FurrBall_RR";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != nodeCount;
        if (needsInit) {
            if (gs.Initialized) {
                for (int i = 0; i < gs.NumaNodeCount; i++) {
                    gs.Workers[i].Stop();
                    gs.Workers[i].~NodeJob();
                }
                free(gs.Workers);
                gs = {};
            }
            gs.NumaNodeCount = nodeCount;
            if (nodeCount > 1) gs.SysNumaPageSize = 65536;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                new(&gs.Workers[i]) NodeJob(i);
                gs.Workers[i].Start([](){});
            }
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = (R == Routing::ThreadLocal);

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <Routing R, typename Policy> int FurrBallAdapter<R, Policy>::runId = 0;

// --- FurrBall TL Strict coherence ---
template <Routing R = Routing::ThreadLocal, typename Policy = ArcPolicy>
struct FurrBallStrictAdapter {
    static constexpr const char* Name = "FurrBall_TL_Strict";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != nodeCount;
        if (needsInit) {
            if (gs.Initialized) {
                for (int i = 0; i < gs.NumaNodeCount; i++) {
                    gs.Workers[i].Stop();
                    gs.Workers[i].~NodeJob();
                }
                free(gs.Workers);
                gs = {};
            }
            gs.NumaNodeCount = nodeCount;
            if (nodeCount > 1) gs.SysNumaPageSize = 65536;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                new(&gs.Workers[i]) NodeJob(i);
                gs.Workers[i].Start([](){});
            }
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_strict_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = (R == Routing::ThreadLocal);

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.StrictCoherence = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <Routing R, typename Policy> int FurrBallStrictAdapter<R, Policy>::runId = 0;

// --- FurrBall TL with Bloom Filter ---
template <Routing R = Routing::ThreadLocal, typename Policy = ArcPolicy>
struct FurrBallBloomAdapter {
    static constexpr const char* Name = "FurrBall_TL_Bloom";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != nodeCount;
        if (needsInit) {
            if (gs.Initialized) {
                for (int i = 0; i < gs.NumaNodeCount; i++) {
                    gs.Workers[i].Stop();
                    gs.Workers[i].~NodeJob();
                }
                free(gs.Workers);
                gs = {};
            }
            gs.NumaNodeCount = nodeCount;
            if (nodeCount > 1) gs.SysNumaPageSize = 65536;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                new(&gs.Workers[i]) NodeJob(i);
                gs.Workers[i].Start([](){});
            }
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_bloom_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = (R == Routing::ThreadLocal);

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.EnableBloomFilter = true;
        fc.BloomFilterBytes = 131072;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <Routing R, typename Policy> int FurrBallBloomAdapter<R, Policy>::runId = 0;

// --- FurrBall TL Strict + Bloom ---
template <Routing R = Routing::ThreadLocal, typename Policy = ArcPolicy>
struct FurrBallStrictBloomAdapter {
    static constexpr const char* Name = "FurrBall_TL_Strict_Bloom";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != nodeCount;
        if (needsInit) {
            if (gs.Initialized) {
                for (int i = 0; i < gs.NumaNodeCount; i++) {
                    gs.Workers[i].Stop();
                    gs.Workers[i].~NodeJob();
                }
                free(gs.Workers);
                gs = {};
            }
            gs.NumaNodeCount = nodeCount;
            if (nodeCount > 1) gs.SysNumaPageSize = 65536;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                new(&gs.Workers[i]) NodeJob(i);
                gs.Workers[i].Start([](){});
            }
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_strict_bloom_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = (R == Routing::ThreadLocal);

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.StrictCoherence = true;
        fc.EnableBloomFilter = true;
        fc.BloomFilterBytes = 131072;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <Routing R, typename Policy> int FurrBallStrictBloomAdapter<R, Policy>::runId = 0;

// --- FurrBall Hash Routed ---
template <Routing R = Routing::ThreadLocal, typename Policy = ArcPolicy>
struct FurrBallHashRoutedAdapter {
    static constexpr const char* Name = "FurrBall_HashRouted";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        bool needsInit = !gs.Initialized || gs.NumaNodeCount != nodeCount;
        if (needsInit) {
            if (gs.Initialized) {
                for (int i = 0; i < gs.NumaNodeCount; i++) {
                    gs.Workers[i].Stop();
                    gs.Workers[i].~NodeJob();
                }
                free(gs.Workers);
                gs = {};
            }
            gs.NumaNodeCount = nodeCount;
            if (nodeCount > 1) gs.SysNumaPageSize = 65536;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                new(&gs.Workers[i]) NodeJob(i);
                gs.Workers[i].Start([](){});
            }
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_hashrouted_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = (R == Routing::ThreadLocal);

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.HashRouted = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <Routing R, typename Policy> int FurrBallHashRoutedAdapter<R, Policy>::runId = 0;

template <typename Policy = ArcPolicy>
struct FurrBallSharedAdapter {
    static constexpr const char* Name = "FurrBall_Shared";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int, int totalCapacityKB) {
        nodeCount = 1;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        if (gs.Initialized && gs.NumaNodeCount != 1) {
            for (int i = 0; i < gs.NumaNodeCount; i++) {
                gs.Workers[i].Stop();
                gs.Workers[i].~NodeJob();
            }
            free(gs.Workers);
            gs = {};
        }
        if (!gs.Initialized) {
            gs.NumaNodeCount = 1;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob));
            new(&gs.Workers[0]) NodeJob(0);
            gs.Workers[0].Start([](){});
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_shared_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <typename Policy> int FurrBallSharedAdapter<Policy>::runId = 0;

template <typename Policy = ArcPolicy>
struct FurrBallNoMaintAdapter {
    static constexpr const char* Name = "FurrBall_NoMaint";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int, int totalCapacityKB) {
        nodeCount = 1;
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        if (gs.Initialized && gs.NumaNodeCount != 1) {
            for (int i = 0; i < gs.NumaNodeCount; i++) {
                gs.Workers[i].Stop();
                gs.Workers[i].~NodeJob();
            }
            free(gs.Workers);
            gs = {};
        }
        if (!gs.Initialized) {
            gs.NumaNodeCount = 1;
            gs.Workers = (NodeJob*)malloc(sizeof(NodeJob));
            new(&gs.Workers[0]) NodeJob(0);
            gs.Workers[0].Start([](){});
            gs.Initialized = true;
        }

        fbPath = "/tmp/ycsb_nomaint_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.SkipMaintenance = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    int numNodes() const { return nodeCount; }
};
template <typename Policy> int FurrBallNoMaintAdapter<Policy>::runId = 0;

// --- FurrBall Single Node ---
template <typename Policy = ArcPolicy>
struct FurrBallSNAdapterT {
    static constexpr const char* Name = "FurrBall_SN";
    static constexpr size_t PAGE_SIZE = 4096;
    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int, int totalCapacityKB) {
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        auto& gs = Detail::globalNumaState;
        if (gs.Initialized) {
            for (int i = 0; i < gs.NumaNodeCount; i++) {
                gs.Workers[i].Stop();
                gs.Workers[i].~NodeJob();
            }
            free(gs.Workers);
            gs = {};
        }

        gs.NumaNodeCount = 1;
        gs.Workers = (NodeJob*)malloc(sizeof(NodeJob));
        new(&gs.Workers[0]) NodeJob(0);
        gs.Workers[0].Start([](){});
        gs.Initialized = true;

        fbPath = "/tmp/ycsb_sn_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.numaConfig = &nc;

        fb = FurrBall<Policy>::CreateBall(fbPath, fc, true);
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Error err = fb->Get(key, buf, bufSize, outSize);
        return (err == NO_ERR && outSize > 0);
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        fb->Set(key, const_cast<uint8_t*>(data), size);
    }

    void destroy() {
        if (fb) { delete fb; fb = nullptr; }
    }

    static int numNodes() { return 1; }
};
template <typename Policy> int FurrBallSNAdapterT<Policy>::runId = 0;
using FurrBallSNAdapter = FurrBallSNAdapterT<ArcPolicy>;

// --- TBB adapter ---
struct TBBAdapter {
    static constexpr const char* Name = "TBB";
    using Map = tbb::concurrent_hash_map<std::string, std::vector<uint8_t>>;
    Map* map = nullptr;
    size_t footprintBytes_ = 0;

    void create(int, int) {
        map = new Map();
        footprintBytes_ = 0;
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        Map::const_accessor acc;
        if (map->find(acc, key)) {
            outSize = acc->second.size();
            if (bufSize >= outSize) {
                memcpy(buf, acc->second.data(), outSize);
            }
            return true;
        }
        return false;
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        Map::accessor acc;
        map->insert(acc, key);
        acc->second.assign(data, data + size);
    }

    void destroy() {
        delete map; map = nullptr;
    }

    static int numNodes() { return 0; }
};

// --- CacheLib adapter ---
#ifdef USE_CACHELIB
#include <cachelib/allocator/CacheAllocator.h>

struct CacheLibAdapter {
    static constexpr const char* Name = "CacheLib";
    using LruAllocator = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;
    static constexpr size_t TARGET_USABLE_BYTES = 32 * 1024 * 1024;

    std::unique_ptr<LruAllocator> cache;
    PoolId pool;
    size_t footprintBytes_ = 0;
    static int runId;
    std::string cacheDir;

    void create(int, int totalCapacityKB) {
        size_t targetBytes = (size_t)totalCapacityKB * 1024;
        if (targetBytes < TARGET_USABLE_BYTES) targetBytes = TARGET_USABLE_BYTES;
        size_t configSize = targetBytes * 2;

        cacheDir = "/tmp/ycsb_cachelib_" + std::to_string(runId++);
        mkdir(cacheDir.c_str(), 0755);

        LruAllocator::Config config;
        config.setCacheName("ycsb_cl");
        config.setCacheSize(configSize);
        config.cacheDir = cacheDir;

        std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024, 2048};
        config.setDefaultAllocSizes(allocSizes);

        cache = std::make_unique<LruAllocator>(config);
        pool = cache->addPool("default", targetBytes);
        footprintBytes_ = configSize;
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        auto handle = cache->find(key);
        if (handle) {
            outSize = handle->getSize();
            if (bufSize >= outSize) {
                memcpy(buf, handle->getMemory(), outSize);
            }
            return true;
        }
        return false;
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        auto handle = cache->allocate(pool, key, size);
        if (handle) {
            memcpy(handle->getMemory(), data, size);
            cache->insertOrReplace(handle);
        }
    }

    void destroy() {
        cache.reset();
        if (!cacheDir.empty()) {
            std::string rm = "rm -rf " + cacheDir;
            system(rm.c_str());
        }
    }

    static int numNodes() { return 0; }
};
int CacheLibAdapter::runId = 0;
#endif

// --- RocksDB LRU Cache adapter ---
struct RocksDBCacheAdapter {
    static constexpr const char* Name = "RocksDB_LRUCache";

    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> cache;
    size_t footprintBytes_ = 0;
    static int runId;

    static void deleter(ROCKSDB_NAMESPACE::Cache::ObjectPtr obj,
                        ROCKSDB_NAMESPACE::MemoryAllocator*) {
        auto* meta = static_cast<std::pair<uint8_t*, size_t>*>(obj);
        delete[] meta->first;
        delete meta;
    }

    static const ROCKSDB_NAMESPACE::Cache::CacheItemHelper* getHelper() {
        static const ROCKSDB_NAMESPACE::Cache::CacheItemHelper helper(
            ROCKSDB_NAMESPACE::CacheEntryRole::kMisc, &deleter);
        return &helper;
    }

    void create(int, int totalCapacityKB) {
        size_t capacity = (size_t)totalCapacityKB * 1024;
        cache = ROCKSDB_NAMESPACE::NewLRUCache(
            capacity,
            -1,
            false,
            0.0);
        footprintBytes_ = capacity;
    }

    bool get(const std::string& key, uint8_t* buf, size_t bufSize, size_t& outSize) {
        auto handle = cache->BasicLookup(ROCKSDB_NAMESPACE::Slice(key), nullptr);
        if (handle) {
            auto* val = static_cast<std::pair<uint8_t*, size_t>*>(cache->Value(handle));
            outSize = val->second;
            if (bufSize >= outSize) {
                memcpy(buf, val->first, outSize);
            }
            cache->Release(handle);
            return true;
        }
        return false;
    }

    void put(const std::string& key, const uint8_t* data, size_t size) {
        auto* buf = new uint8_t[size];
        memcpy(buf, data, size);
        auto* meta = new std::pair<uint8_t*, size_t>(buf, size);

        auto helper = getHelper();
        ROCKSDB_NAMESPACE::Cache::Handle* h = nullptr;
        auto s = cache->Insert(
            ROCKSDB_NAMESPACE::Slice(key),
            meta,
            helper,
            size + sizeof(std::pair<uint8_t*, size_t>),
            &h,
            ROCKSDB_NAMESPACE::Cache::Priority::LOW);
        if (h) {
            cache->Release(h);
        }
        if (!s.ok()) {
            delete[] buf;
            delete meta;
        }
    }

    void destroy() {
        cache.reset();
    }

    static int numNodes() { return 0; }
};
int RocksDBCacheAdapter::runId = 0;

// ============================================================================
//  YCSB runner
// ============================================================================

template <typename System>
static ThreadResult runYCSBWorker(
    System& sys,
    YCSBWorkload wl,
    size_t opsPerThread,
    uint64_t recordCount,
    size_t valueSize,
    int numaNode,
    int threadIndex,
    int totalThreads)
{
    ThreadResult result;
    result.numaNode = numaNode;

    if (numaNode >= 0) {
        Numatic::PinCurrentThreadToNode(numaNode);
    }

    uint8_t outBuf[8192];
    std::vector<uint8_t> val(valueSize, 'x');

    // Phase 1: Load — insert recordCount/totalThreads records per thread
    uint64_t myRecords = recordCount / totalThreads;
    uint64_t myBase = (uint64_t)threadIndex * myRecords;
    for (uint64_t i = 0; i < myRecords; i++) {
        std::string key = "ycsb_" + std::to_string(myBase + i);
        sys.put(key, val.data(), valueSize);
    }

    // Phase 2: Run — execute YCSB workload
    auto ops = generateYCSBWorkload(wl, opsPerThread, recordCount, threadIndex, totalThreads);

    for (const auto& op : ops) {
        std::string key = "ycsb_" + std::to_string(op.keyIndex);

        if (op.isRead) {
            size_t outSize = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            bool found = sys.get(key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();
            result.getLatencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            result.gets++;
            if (found) {
                result.hits++;
            } else {
                result.misses++;
            }
        } else {
            // Update (or RMW — we model RMW as update for simplicity)
            auto t0 = std::chrono::high_resolution_clock::now();
            sys.put(key, val.data(), valueSize);
            auto t1 = std::chrono::high_resolution_clock::now();
            result.setLatencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            result.sets++;
        }
    }

    return result;
}

// ============================================================================
//  Benchmark fixture
// ============================================================================

template <typename System>
struct YCSBBench : public benchmark::Fixture {
    static constexpr size_t OPS_PER_THREAD = 200000;
};

template <typename System>
void RunYCSBBench(benchmark::State& state) {
    int numThreads = state.range(0);
    int totalCapacityKB = state.range(1);
    YCSBWorkload wl = static_cast<YCSBWorkload>(state.range(2));
    size_t valueSize = state.range(3);
    uint64_t recordCount = state.range(4);

    std::vector<IterStats> iterStats;

    for (auto _ : state) {
        state.PauseTiming();

        System sys;
        sys.create(numThreads, totalCapacityKB);

        std::vector<std::thread> threads;
        std::vector<ThreadResult> results(numThreads);

        state.ResumeTiming();

        auto wallStart = std::chrono::high_resolution_clock::now();

        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&, t]() {
                int nn = sys.numNodes();
                results[t] = runYCSBWorker<System>(
                    sys, wl, YCSBBench<System>::OPS_PER_THREAD,
                    recordCount, valueSize,
                    nn > 0 ? (t % nn) : -1,
                    t, numThreads);
            });
        }

        for (auto& th : threads) th.join();

        auto wallEnd = std::chrono::high_resolution_clock::now();
        double elapsedSec = std::chrono::duration<double>(wallEnd - wallStart).count();

        state.PauseTiming();

        AggregateResult ar = aggregate(results, elapsedSec);

        IterStats is;
        is.p50Get = ar.p50GetNs; is.p90Get = ar.p90GetNs; is.p99Get = ar.p99GetNs;
        is.p50Set = ar.p50SetNs; is.p90Set = ar.p90SetNs; is.p99Set = ar.p99SetNs;
        is.opsPerSec = ar.opsPerSec; is.hitRate = ar.hitRatePct;
        iterStats.push_back(is);

        size_t runIdx = iterStats.size();
        if (runIdx == state.max_iterations) {
            reportAggStats(state, iterStats, runIdx);
            for (size_t n = 0; n < ar.perNodeP50Get.size(); n++) {
                std::string p50name = "node" + std::to_string(n) + "_p50_get_ns";
                state.counters[p50name] = ar.perNodeP50Get[n];
            }
        } else {
            reportAgg(state, ar);
        }

        state.counters["footprint_mb"] = (double)sys.footprintBytes_ / (1024.0 * 1024.0);
        state.counters["runs"] = (double)runIdx;

        sys.destroy();

        state.ResumeTiming();
    }
}

// ============================================================================
//  Registrations
// ============================================================================

static const char* ycsbName(int w) {
    switch (w) {
    case 10: return "YCSB_A";
    case 11: return "YCSB_B";
    case 12: return "YCSB_C";
    case 13: return "YCSB_F";
    default: return "?";
    }
}

// --- FurrBall TL YCSB ---
struct YCSB_FurrBallTL : YCSBBench<FurrBallAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallTL, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallAdapter<Routing::ThreadLocal>>(state);
}

// --- FurrBall TL Strict YCSB ---
struct YCSB_FurrBallTLStrict : YCSBBench<FurrBallStrictAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallTLStrict, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallStrictAdapter<Routing::ThreadLocal>>(state);
}

// --- FurrBall TL Bloom YCSB ---
struct YCSB_FurrBallTLBloom : YCSBBench<FurrBallBloomAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallTLBloom, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallBloomAdapter<Routing::ThreadLocal>>(state);
}

// --- FurrBall TL Strict+Bloom YCSB ---
struct YCSB_FurrBallTLStrictBloom : YCSBBench<FurrBallStrictBloomAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallTLStrictBloom, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallStrictBloomAdapter<Routing::ThreadLocal>>(state);
}

// --- FurrBall Hash Routed YCSB ---
struct YCSB_FurrBallHashRouted : YCSBBench<FurrBallHashRoutedAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallHashRouted, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallHashRoutedAdapter<Routing::ThreadLocal>>(state);
}

// --- FurrBall SN YCSB ---
struct YCSB_FurrBallSN : YCSBBench<FurrBallSNAdapter> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallSN, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallSNAdapter>(state);
}

// --- FurrBall Shared (non-NUMA safe) YCSB ---
struct YCSB_FurrBallShared : YCSBBench<FurrBallSharedAdapter<ArcPolicy>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallShared, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallSharedAdapter<ArcPolicy>>(state);
}

// --- FurrBall NoMaint (shared, no background maintenance) YCSB ---
struct YCSB_FurrBallNoMaint : YCSBBench<FurrBallNoMaintAdapter<ArcPolicy>> {};
BENCHMARK_DEFINE_F(YCSB_FurrBallNoMaint, Run)(benchmark::State& state) {
    RunYCSBBench<FurrBallNoMaintAdapter<ArcPolicy>>(state);
}

// --- TBB YCSB ---
struct YCSB_TBB : YCSBBench<TBBAdapter> {};
BENCHMARK_DEFINE_F(YCSB_TBB, Run)(benchmark::State& state) {
    RunYCSBBench<TBBAdapter>(state);
}

// --- RocksDB YCSB ---
struct YCSB_RocksDB : YCSBBench<RocksDBCacheAdapter> {};
BENCHMARK_DEFINE_F(YCSB_RocksDB, Run)(benchmark::State& state) {
    RunYCSBBench<RocksDBCacheAdapter>(state);
}

#ifdef USE_CACHELIB
// --- CacheLib YCSB ---
struct YCSB_CacheLib : YCSBBench<CacheLibAdapter> {};
BENCHMARK_DEFINE_F(YCSB_CacheLib, Run)(benchmark::State& state) {
    RunYCSBBench<CacheLibAdapter>(state);
}
#endif

// ============================================================================
//  Registration matrix:
//    5 systems x 3 workloads (A/B/C) x 2 thread counts (1T/2T)
//    = 30 benchmarks
//    + valueSize=1024 sweep: 5 x 3 x 1 = 15
//    = 45 total
//
//    recordCount=100000 (100K keys)
//    totalCapacityKB=32768 (32MB)
// ============================================================================

// ============================================================================
//  1-thread / 32MB / A/B/C / 64B — non-NUMA baseline
// ============================================================================

BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({1, 32768, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({1, 32768, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({1, 32768, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallNoMaint, Run)
    ->Args({1, 32768, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallNoMaint, Run)
    ->Args({1, 32768, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallNoMaint, Run)
    ->Args({1, 32768, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({1, 32768, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({1, 32768, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({1, 32768, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({1, 32768, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({1, 32768, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({1, 32768, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

// --- YCSB A (50R/50W), 64B, 2T ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 10, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 10, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 10, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 10, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 10, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- YCSB B (95R/5W), 64B ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- YCSB C (100R), 64B ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 12, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 12, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 12, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 12, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 12, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- YCSB A/B/C at 256B (100K × 256B = 25MB, light oversubscription vs 32MB cache) ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 10, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 11, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 12, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 10, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 11, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 12, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 10, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 11, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 12, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 10, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 11, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 12, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 10, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 11, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 12, 256, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- YCSB A/B/C at 1024B (100K × 1024B = 100MB, heavy oversubscription vs 32MB cache) ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 10, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 11, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 32768, 12, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 10, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 11, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 32768, 12, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 10, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 11, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 32768, 12, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 10, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 11, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 32768, 12, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 10, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 11, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 32768, 12, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- Capacity sweep: 64MB, 128MB, 512MB (64B, 2T+4T) ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({2, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({4, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({2, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({4, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({2, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({4, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({2, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({4, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({2, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 131072, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 131072, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 131072, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 524288, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 524288, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({4, 524288, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
#endif

// --- 1T baselines ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTL, Run)
    ->Args({1, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_FurrBallSN, Run)
    ->Args({1, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_TBB, Run)
    ->Args({1, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(YCSB_RocksDB, Run)
    ->Args({1, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(YCSB_CacheLib, Run)
    ->Args({1, 32768, 11, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- FurrBall TL Strict: 4T/64MB, A/B/C ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrict, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrict, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrict, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

// --- FurrBall TL Bloom: 4T/64MB, A/B/C ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTLBloom, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLBloom, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLBloom, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

// --- FurrBall TL Strict+Bloom: 4T/64MB, A/B/C ---
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrictBloom, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrictBloom, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallTLStrictBloom, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

// --- FurrBall Hash Routed: 4T/64MB, A/B/C ---
BENCHMARK_REGISTER_F(YCSB_FurrBallHashRouted, Run)
    ->Args({4, 65536, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallHashRouted, Run)
    ->Args({4, 65536, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallHashRouted, Run)
    ->Args({4, 65536, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

// --- FurrBall Shared (non-NUMA safe): 2T/32MB, A/B/C ---
BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({2, 32768, 10, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({2, 32768, 11, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);
BENCHMARK_REGISTER_F(YCSB_FurrBallShared, Run)
    ->Args({2, 32768, 12, 64, 100000})->Iterations(10)->Unit(benchmark::kMicrosecond);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    auto& gs = Detail::globalNumaState;
    if (gs.Initialized && gs.Workers) {
        for (int i = 0; i < gs.NumaNodeCount; i++) {
            gs.Workers[i].Stop();
            gs.Workers[i].~NodeJob();
        }
        free(gs.Workers);
        gs = {};
    }

    return 0;
}
