// NUMABench.cpp — Multi-threaded NUMA topology benchmark
//
// Tests whether topology-aware key placement (FurrBall ThreadLocal/RoundRobin)
// improves latency over NUMA-blind alternatives (TBB, cachelib).
//
// Architecture:
//   System adapters   — thin wrappers: put/get/create/destroy
//   Workload generators — partitioned keys, shared keys, trace replay
//   Thread manager    — pin N threads to N NUMA nodes, per-thread metrics
//
// Design choices:
//   - Each system is a compile-time template parameter (no virtual dispatch)
//   - Per-thread latency vectors enable local-vs-remote analysis
//   - cachelib adapter is a placeholder that #ifdef-guards the real impl

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
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

using namespace NuAtlas;

// ============================================================================
//  Shared utilities
// ============================================================================

static std::string getTracePath() {
    const char* env = std::getenv("FURRBALL_TRACE");
    if (env && env[0]) return std::string(env);
    return "/home/ubuntu/source/repos/libCacheSim/data/twitter_cluster52.csv";
}

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

// Per-thread result
struct ThreadResult {
    size_t hits = 0;
    size_t misses = 0;
    size_t gets = 0;
    size_t sets = 0;
    std::vector<int64_t> getLatencies;
    std::vector<int64_t> setLatencies;
    int numaNode = -1;
};

// Aggregate results from all threads
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

    // Per-node p50/p90/p99 (for local vs remote analysis)
    std::vector<double> perNodeP50Get;
    std::vector<double> perNodeP90Get;
    std::vector<double> perNodeP99Get;
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
    ar.perNodeP90Get.resize(maxNode + 1, 0);
    ar.perNodeP99Get.resize(maxNode + 1, 0);
    for (const auto& r : results) {
        if (r.numaNode >= 0 && !r.getLatencies.empty()) {
            auto lats = r.getLatencies;
            std::sort(lats.begin(), lats.end());
            ar.perNodeP50Get[r.numaNode] = (double)lats[lats.size() / 2];
            ar.perNodeP90Get[r.numaNode] = (double)lats[lats.size() * 90 / 100];
            ar.perNodeP99Get[r.numaNode] = (double)lats[lats.size() * 99 / 100];
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
        std::string p90name = "node" + std::to_string(n) + "_p90_get_ns";
        std::string p99name = "node" + std::to_string(n) + "_p99_get_ns";
        state.counters[p50name] = ar.perNodeP50Get[n];
        state.counters[p90name] = ar.perNodeP90Get[n];
        state.counters[p99name] = ar.perNodeP99Get[n];
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
    double d;
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

// ============================================================================
//  Workload generators
//
//  Each generates a sequence of (key_string, should_insert) operations.
//  Partitioned: thread T gets keys [T * rangeSize, (T+1) * rangeSize)
//  Shared: all threads use same key space
//  Trace: replay Twitter trace across all threads (interleaved)
// ============================================================================

enum class WorkloadType { Partitioned, Shared, Trace, ReadOnly, UniformRO };

struct Op {
    std::string key;
    bool isRead;
};

struct WorkloadConfig {
    WorkloadType type;
    size_t opsPerThread;
    uint64_t zipfianUniverse;
    double zipfianTheta;
    int threadIndex;
    int totalThreads;
    const std::vector<TraceEntry>* trace;
};

static std::vector<Op> generateWorkload(const WorkloadConfig& cfg) {
    std::vector<Op> ops;
    ops.reserve(cfg.opsPerThread);
    uint64_t zState = 42 + cfg.threadIndex * 1000;

    switch (cfg.type) {
    case WorkloadType::Partitioned: {
        uint64_t rangeSize = cfg.zipfianUniverse / cfg.totalThreads;
        uint64_t base = (uint64_t)cfg.threadIndex * rangeSize;
        for (size_t i = 0; i < cfg.opsPerThread; i++) {
            uint64_t ki = base + (zipfian(rangeSize, cfg.zipfianTheta, zState) % rangeSize);
            ops.push_back({"p_" + std::to_string(ki), true});
        }
        break;
    }
    case WorkloadType::Shared: {
        for (size_t i = 0; i < cfg.opsPerThread; i++) {
            uint64_t ki = zipfian(cfg.zipfianUniverse, cfg.zipfianTheta, zState);
            ops.push_back({"s_" + std::to_string(ki), true});
        }
        break;
    }
    case WorkloadType::Trace: {
        if (!cfg.trace || cfg.trace->empty()) {
            for (size_t i = 0; i < cfg.opsPerThread; i++) {
                uint64_t ki = zipfian(cfg.zipfianUniverse, cfg.zipfianTheta, zState);
                ops.push_back({"s_" + std::to_string(ki), true});
            }
        } else {
            size_t stride = (size_t)cfg.totalThreads;
            size_t start = (size_t)cfg.threadIndex;
            for (size_t i = 0; i < cfg.opsPerThread; i++) {
                size_t idx = (start + i * stride) % cfg.trace->size();
                uint64_t ki = (*cfg.trace)[idx].key;
                ops.push_back({"tr_" + std::to_string(ki), true});
            }
        }
        break;
    }
    case WorkloadType::ReadOnly: {
        uint64_t rangeSize = cfg.zipfianUniverse / cfg.totalThreads;
        uint64_t base = (uint64_t)cfg.threadIndex * rangeSize;
        for (size_t i = 0; i < cfg.opsPerThread; i++) {
            uint64_t ki = base + (zipfian(rangeSize, cfg.zipfianTheta, zState) % rangeSize);
            ops.push_back({"p_" + std::to_string(ki), true});
        }
        break;
    }
    case WorkloadType::UniformRO: {
        uint64_t rangeSize = cfg.zipfianUniverse / cfg.totalThreads;
        uint64_t base = (uint64_t)cfg.threadIndex * rangeSize;
        std::mt19937_64 rng(42 + cfg.threadIndex * 1000);
        for (size_t i = 0; i < cfg.opsPerThread; i++) {
            uint64_t ki = base + (rng() % rangeSize);
            ops.push_back({"p_" + std::to_string(ki), true});
        }
        break;
    }
    }
    return ops;
}

// ============================================================================
//  System adapters
//
//  Each adapter is a struct with:
//    static constexpr const char* Name
//    create(nodeCount, pagesPerNode, routing) — initialize
//    put(key, data, size) -> bool (true = insert, false = update)
//    get(key, buf, bufSize, outSize) -> bool
//    destroy()
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
    int pageCount = 64;
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

        fbPath = "/tmp/numabench_" + std::to_string(runId++);

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

// --- FurrBall Single Node (no NUMA baseline) ---

template <typename Policy = ArcPolicy>
struct FurrBallSNAdapterT {
    static constexpr const char* Name = "FurrBall_SN";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        (void)nodeCount;
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

        fbPath = "/tmp/numabench_sn_" + std::to_string(runId++);

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


// --- FurrBall cross-node adapter (all data on node 0, threads on node 0+1) ---

struct FurrBallCNAdapter {
    static constexpr const char* Name = "FurrBall_CN";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<ArcPolicy>* fb = nullptr;
    std::string fbPath;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        auto& gs = Detail::globalNumaState;
        if (gs.Initialized) {
            for (int i = 0; i < gs.NumaNodeCount; i++) {
                gs.Workers[i].Stop();
                gs.Workers[i].~NodeJob();
            }
            free(gs.Workers);
            gs = {};
        }

        int actualNodes = std::max(nodeCount, 2);
        gs.NumaNodeCount = actualNodes;
        if (actualNodes > 1) gs.SysNumaPageSize = 65536;
        gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * actualNodes);
        for (int i = 0; i < actualNodes; i++) {
            new(&gs.Workers[i]) NodeJob(i);
            gs.Workers[i].Start([](){});
        }
        gs.Initialized = true;

        fbPath = "/tmp/numabench_cn_" + std::to_string(runId++);
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.TotalCapacityBytes = (size_t)totalCapacityKB * 1024;
        fc.IsVolatile = true;
        fc.EnableNUMA = true;
        fc.numaConfig = &nc;

        fb = FurrBall<ArcPolicy>::CreateBall(fbPath, fc, true);
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

    static int numNodes() { return 2; }
};

int FurrBallCNAdapter::runId = 0;

// --- FurrBall cross-node LRU adapter ---

template <typename Policy = ArcPolicy>
struct FurrBallCNAdapterT {
    static constexpr const char* Name = "FurrBall_CN";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        auto& gs = Detail::globalNumaState;
        if (gs.Initialized) {
            for (int i = 0; i < gs.NumaNodeCount; i++) {
                gs.Workers[i].Stop();
                gs.Workers[i].~NodeJob();
            }
            free(gs.Workers);
            gs = {};
        }

        int actualNodes = std::max(nodeCount, 2);
        gs.NumaNodeCount = actualNodes;
        if (actualNodes > 1) gs.SysNumaPageSize = 65536;
        gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * actualNodes);
        for (int i = 0; i < actualNodes; i++) {
            new(&gs.Workers[i]) NodeJob(i);
            gs.Workers[i].Start([](){});
        }
        gs.Initialized = true;

        fbPath = "/tmp/numabench_cn_" + std::to_string(runId++);
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

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

    static int numNodes() { return 2; }
};

template <typename Policy> int FurrBallCNAdapterT<Policy>::runId = 0;

using FurrBallLRUCNAdapter = FurrBallCNAdapterT<LruPolicy>;

// --- FurrBall Remote-Only adapter ---
// All data on node 0. Thread 1 pinned to node 1 reads remote.
// Thread 0 is local (node 0). Thread 1 is 100% remote.
// Per-node p50 reveals: node0=local, node1=remote — no blending.

template <typename Policy = ArcPolicy>
struct FurrBallRemoteAdapter {
    static constexpr const char* Name = "FurrBall_Remote";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<Policy>* fb = nullptr;
    std::string fbPath;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int nodeCount, int totalCapacityKB) {
        (void)nodeCount;
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

        fbPath = "/tmp/numabench_remote_" + std::to_string(runId++);
        footprintBytes_ = (size_t)totalCapacityKB * 1024;

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

    static int numNodes() { return 2; }
};

template <typename Policy> int FurrBallRemoteAdapter<Policy>::runId = 0;

using FurrBallLRURemoteAdapter = FurrBallRemoteAdapter<LruPolicy>;

// --- TBB concurrent_hash_map adapter ---

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

// --- cachelib adapter (default, no NUMA binding) ---
#ifdef USE_CACHELIB
#include <cachelib/allocator/CacheAllocator.h>

struct CacheLibAdapter {
    static constexpr const char* Name = "CacheLib";

    using LruAllocator = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;

    static constexpr size_t PAGE_SIZE = 4096;

    static constexpr size_t TARGET_USABLE_BYTES = 32 * 1024 * 1024;

    std::unique_ptr<LruAllocator> cache;
    PoolId pool;
    size_t footprintBytes_ = 0;
    static int runId;

    void create(int, int totalCapacityKB) {
        size_t targetBytes = (size_t)totalCapacityKB * 1024;
        if (targetBytes < TARGET_USABLE_BYTES) targetBytes = TARGET_USABLE_BYTES;

        size_t configSize = targetBytes * 2;

        LruAllocator::Config config;
        config.setCacheName("numabench_cl");
        config.setCacheSize(configSize);

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
    }

    static int numNodes() { return 0; }
};
int CacheLibAdapter::runId = 0;

// --- cachelib NUMA-aware adapter (per-pool setMemBind) ---
// Two pools, each bound to a NUMA node. Uses thread_local pool routing.

#include <cachelib/allocator/CacheAllocator.h>

struct CacheLibNumaAdapter {
    static constexpr const char* Name = "CacheLib_NUMA";

    using LruAllocator = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;

    static constexpr size_t PAGE_SIZE = 4096;

    static constexpr size_t TARGET_USABLE_BYTES = 32 * 1024 * 1024;

    std::unique_ptr<LruAllocator> cache;
    PoolId pools[2];
    size_t footprintBytes_ = 0;
    static int runId;
    std::string cacheDir;
    int nodeCount;
    static inline thread_local int currentPoolIdx = 0;

    void create(int nodeCount, int totalCapacityKB) {
        this->nodeCount = nodeCount;
        size_t totalTarget = (size_t)totalCapacityKB * 1024;
        if (totalTarget < TARGET_USABLE_BYTES) totalTarget = TARGET_USABLE_BYTES;

        size_t perPoolUsable = totalTarget / std::max(nodeCount, 1);
        size_t perPoolConfig = perPoolUsable * 2;
        size_t totalConfig = perPoolConfig * std::max(nodeCount, 1);

        LruAllocator::Config config;
        config.setCacheName("numabench_cl_numa");
        config.setCacheSize(totalConfig);

        std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024, 2048};
        config.setDefaultAllocSizes(allocSizes);

        cache = std::make_unique<LruAllocator>(config);

        if (nodeCount >= 2) {
            pools[0] = cache->addPool("node0", perPoolUsable);
            pools[1] = cache->addPool("node1", perPoolUsable);
        } else {
            pools[0] = cache->addPool("node0", perPoolUsable);
            pools[1] = pools[0];
        }
        footprintBytes_ = totalConfig;
    }

    void setThreadPool(int threadIdx) {
        currentPoolIdx = (nodeCount >= 2) ? std::min(threadIdx, 1) : 0;
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
        auto handle = cache->allocate(pools[currentPoolIdx], key, size);
        if (handle) {
            memcpy(handle->getMemory(), data, size);
            cache->insertOrReplace(handle);
        }
    }

    void destroy() {
        cache.reset();
    }

    static int numNodes() { return 0; }
};
int CacheLibNumaAdapter::runId = 0;
#endif

// ============================================================================
//  Thread runner — pins to NUMA node, runs workload, returns per-thread result
// ============================================================================

template <typename System>
static ThreadResult runWorker(
    System& sys,
    const WorkloadConfig& cfg,
    int numaNode,
    size_t valueSize)
{
    ThreadResult result;
    result.numaNode = numaNode;

    if (numaNode >= 0) {
        Numatic::PinCurrentThreadToNode(numaNode);
    }

    uint8_t outBuf[8192];
    std::vector<uint8_t> val(valueSize, 'x');

    if (cfg.type == WorkloadType::ReadOnly || cfg.type == WorkloadType::UniformRO) {
        uint64_t zState = 42 + cfg.threadIndex * 1000;
        uint64_t rangeSize = cfg.zipfianUniverse / cfg.totalThreads;
        uint64_t base = (uint64_t)cfg.threadIndex * rangeSize;
        if (cfg.type == WorkloadType::UniformRO) {
            std::mt19937_64 rng(42 + cfg.threadIndex * 1000);
            for (size_t i = 0; i < cfg.opsPerThread; i++) {
                uint64_t ki = base + (rng() % rangeSize);
                std::string key = "p_" + std::to_string(ki);
                sys.put(key, val.data(), valueSize);
            }
        } else {
            for (size_t i = 0; i < cfg.opsPerThread; i++) {
                uint64_t ki = base + (zipfian(rangeSize, cfg.zipfianTheta, zState) % rangeSize);
                std::string key = "p_" + std::to_string(ki);
                sys.put(key, val.data(), valueSize);
            }
        }
    }

    for (const auto& op : generateWorkload(cfg)) {
        if (op.isRead) {
            size_t outSize = 0;
            auto t0 = std::chrono::high_resolution_clock::now();
            bool found = sys.get(op.key, outBuf, sizeof(outBuf), outSize);
            auto t1 = std::chrono::high_resolution_clock::now();
            result.getLatencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            result.gets++;
            if (found) {
                result.hits++;
            } else if (cfg.type != WorkloadType::ReadOnly && cfg.type != WorkloadType::UniformRO) {
                result.misses++;
                auto ts0 = std::chrono::high_resolution_clock::now();
                sys.put(op.key, val.data(), valueSize);
                auto ts1 = std::chrono::high_resolution_clock::now();
                result.setLatencies.push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(ts1 - ts0).count());
                result.sets++;
            }
        }
    }

    return result;
}

// ============================================================================
//  Benchmark fixture — parameterized by system, workload, thread count
// ============================================================================

// Args encoding (Google Benchmark only supports int64_t args):
//   state.range(0) = numThreads (== numNodes for ThreadLocal routing)
//   state.range(1) = totalCapacityKB (total budget in KB, auto-split across nodes)
//   state.range(2) = workload type (0=Partitioned, 1=Shared, 2=Trace, 3=ReadOnly)
//   state.range(3) = value size in bytes (64, 256, 1024)

static const char* workloadName(int w) {
    switch (w) {
    case 0: return "Partitioned";
    case 1: return "Shared";
    case 2: return "Trace";
    case 3: return "ReadOnly";
    case 4: return "UniformRO";
    default: return "?";
    }
}

template <typename System>
struct NUMABench : public benchmark::Fixture {
    std::vector<TraceEntry> trace;

    static constexpr size_t OPS_PER_THREAD = 100000;

    void SetUp(benchmark::State& state) override {
        state.PauseTiming();
        if (trace.empty()) {
            trace = loadTrace(getTracePath(), 2000000);
            fprintf(stderr, "[NUMABench/%s] Loaded %zu trace entries\n",
                    System::Name, trace.size());
        }
        state.ResumeTiming();
    }

    void TearDown(benchmark::State& state) override {}
};

template <typename System>
void RunNUMABench(benchmark::State& state, std::vector<TraceEntry>& trace) {
    int numThreads = state.range(0);
    int totalCapacityKB = state.range(1);
    int workloadType = state.range(2);
    size_t valueSize = state.range(3);

    std::vector<IterStats> iterStats;

    for (auto _ : state) {
        state.PauseTiming();

        System sys;
        sys.create(numThreads, totalCapacityKB);

        std::vector<std::thread> threads;
        std::vector<ThreadResult> results(numThreads);

        auto wallStart = std::chrono::high_resolution_clock::now();

        state.ResumeTiming();

        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&, t]() {
                if constexpr (requires(System& s, int i) { s.setThreadPool(i); }) {
                    sys.setThreadPool(t);
                }

                WorkloadConfig cfg;
                cfg.type = static_cast<WorkloadType>(workloadType);
                cfg.opsPerThread = NUMABench<System>::OPS_PER_THREAD;
                cfg.zipfianUniverse = state.range(4);
                cfg.zipfianTheta = 0.99;
                cfg.threadIndex = t;
                cfg.totalThreads = numThreads;
                cfg.trace = &trace;

                int nn = sys.numNodes();
                results[t] = runWorker(sys, cfg, nn > 0 ? (t % nn) : -1, valueSize);
            });
        }

        for (auto& th : threads) th.join();

        state.PauseTiming();

        auto wallEnd = std::chrono::high_resolution_clock::now();
        double elapsedSec = std::chrono::duration<double>(wallEnd - wallStart).count();

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
                std::string p90name = "node" + std::to_string(n) + "_p90_get_ns";
                std::string p99name = "node" + std::to_string(n) + "_p99_get_ns";
                state.counters[p50name] = ar.perNodeP50Get[n];
                state.counters[p90name] = ar.perNodeP90Get[n];
                state.counters[p99name] = ar.perNodeP99Get[n];
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
//  Benchmark registrations
//
//  Full matrix: 6 systems × {1,2,4} threads × {16,64} pages × 3 workloads
//  = 6 × 3 × 2 × 3 = 108 benchmarks.
//  10 iterations per benchmark for statistical stability.
//
//  Systems:
//    FurrBall_TL   — topology-aware routing (thread-local)
//    FurrBall_RR   — NUMA-blind routing (round-robin)
//    FurrBall_SN   — single-node, no NUMA (pure data structure baseline)
//    FurrBall_CN   — cross-node (all data on node 0, threads on node 0+1)
//    TBB           — concurrent_hash_map (unbounded, no eviction)
//    CacheLib      — production LRU (default, accidental interleaving)
//    CacheLib_NUMA — production LRU (NUMA-aware, per-pool routing)
//
//  Key comparisons enabled:
//    SN vs TBB vs CacheLib     → pure access path (no NUMA)
//    CN vs SN                  → raw NUMA memory penalty
//    TL vs CN                  → how much placement recovers
//    TL vs CacheLib_NUMA       → fair NUMA-aware fight
//    CacheLib vs CacheLib_NUMA → does NUMA awareness help CacheLib?
// ============================================================================

// --- FurrBall ThreadLocal ---

struct NUMABench_FurrBallTL : NUMABench<FurrBallAdapter<Routing::ThreadLocal>> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallTL, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallAdapter<Routing::ThreadLocal>>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({1, 512, 0, 64, 100000})
    ->Args({1, 512, 1, 64, 100000})
    ->Args({2, 512, 0, 64, 100000})
    ->Args({2, 512, 1, 64, 100000})
    ->Args({2, 512, 2, 64, 100000})
    ->Args({2, 128, 0, 64, 100000})
    ->Args({2, 128, 1, 64, 100000})
    ->Args({2, 128, 2, 64, 100000})
    ->Args({4, 512, 0, 64, 100000})
    ->Args({4, 512, 1, 64, 100000})
    ->Args({4, 512, 2, 64, 100000})
    ->Args({4, 128, 0, 64, 100000})
    ->Args({4, 128, 1, 64, 100000})
    ->Args({4, 128, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall RoundRobin ---

struct NUMABench_FurrBallRR : NUMABench<FurrBallAdapter<Routing::RoundRobin>> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallRR, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallAdapter<Routing::RoundRobin>>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({1, 512, 0, 64, 100000})
    ->Args({1, 512, 1, 64, 100000})
    ->Args({2, 512, 0, 64, 100000})
    ->Args({2, 512, 1, 64, 100000})
    ->Args({2, 512, 2, 64, 100000})
    ->Args({2, 128, 0, 64, 100000})
    ->Args({2, 128, 1, 64, 100000})
    ->Args({2, 128, 2, 64, 100000})
    ->Args({4, 512, 0, 64, 100000})
    ->Args({4, 512, 1, 64, 100000})
    ->Args({4, 512, 2, 64, 100000})
    ->Args({4, 128, 0, 64, 100000})
    ->Args({4, 128, 1, 64, 100000})
    ->Args({4, 128, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall Single Node (no NUMA baseline) ---

struct NUMABench_FurrBallSN : NUMABench<FurrBallSNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallSN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallSNAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({1, 256, 0, 64, 100000})
    ->Args({1, 256, 1, 64, 100000})
    ->Args({2, 256, 0, 64, 100000})
    ->Args({2, 256, 1, 64, 100000})
    ->Args({2, 256, 2, 64, 100000})
    ->Args({2, 64, 0, 64, 100000})
    ->Args({2, 64, 1, 64, 100000})
    ->Args({2, 64, 2, 64, 100000})
    ->Args({4, 256, 0, 64, 100000})
    ->Args({4, 256, 1, 64, 100000})
    ->Args({4, 256, 2, 64, 100000})
    ->Args({4, 64, 0, 64, 100000})
    ->Args({4, 64, 1, 64, 100000})
    ->Args({4, 64, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall Cross Node (all data on node 0, threads on 0+1) ---

struct NUMABench_FurrBallCN : NUMABench<FurrBallCNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallCN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallCNAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 512, 0, 64, 100000})
    ->Args({2, 512, 1, 64, 100000})
    ->Args({2, 512, 2, 64, 100000})
    ->Args({2, 128, 0, 64, 100000})
    ->Args({2, 128, 1, 64, 100000})
    ->Args({2, 128, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- TBB concurrent_hash_map ---

struct NUMABench_TBB : NUMABench<TBBAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_TBB, Run)(benchmark::State& state) {
    RunNUMABench<TBBAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({1, 256, 0, 64, 100000})
    ->Args({1, 256, 1, 64, 100000})
    ->Args({2, 256, 0, 64, 100000})
    ->Args({2, 256, 1, 64, 100000})
    ->Args({2, 256, 2, 64, 100000})
    ->Args({2, 64, 0, 64, 100000})
    ->Args({2, 64, 1, 64, 100000})
    ->Args({2, 64, 2, 64, 100000})
    ->Args({4, 256, 0, 64, 100000})
    ->Args({4, 256, 1, 64, 100000})
    ->Args({4, 256, 2, 64, 100000})
    ->Args({4, 64, 0, 64, 100000})
    ->Args({4, 64, 1, 64, 100000})
    ->Args({4, 64, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall LRU ThreadLocal ---

using FurrBallLRUTLAdapter = FurrBallAdapter<Routing::ThreadLocal, LruPolicy>;
using FurrBallLRUSNAdapter = FurrBallSNAdapterT<LruPolicy>;

struct NUMABench_FurrBallLRUTL : NUMABench<FurrBallLRUTLAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallLRUTL, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallLRUTLAdapter>(state, trace);
}

// --- FurrBall LRU SingleNode ---

struct NUMABench_FurrBallLRUSN : NUMABench<FurrBallLRUSNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallLRUSN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallLRUSNAdapter>(state, trace);
}

// --- FurrBall LRU Cross-Node ---

struct NUMABench_FurrBallLRUCN : NUMABench<FurrBallLRUCNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallLRUCN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallLRUCNAdapter>(state, trace);
}

// --- FurrBall Remote-Only (ARC) ---

struct NUMABench_FurrBallRemote : NUMABench<FurrBallRemoteAdapter<ArcPolicy>> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallRemote, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallRemoteAdapter<ArcPolicy>>(state, trace);
}

// --- FurrBall Remote-Only (LRU) ---

struct NUMABench_FurrBallLRURemote : NUMABench<FurrBallLRURemoteAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallLRURemote, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallLRURemoteAdapter>(state, trace);
}

// Equal-capacity LRU benchmarks (32MB budget, 700K universe)
// LRU_TL: totalCapacityKB=32768 always
// LRU_SN: totalCapacityKB=32768 always

// --- LRU Equal cap: Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: Trace 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: ReadOnly 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 3, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 3, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: Partitioned 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: Shared 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU Equal cap: Single-thread baseline ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Equal-capacity 1024B value benchmarks — NUMA-locality evidence
//  At 1KB values, remote memcpy (~200ns) becomes visible vs lock overhead.
//  ReadOnly (workload=3) eliminates eviction confound for pure access path.
// ============================================================================

// --- ARC 1024B: Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU 1024B: Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC 1024B: Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU 1024B: Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC 1024B: 1t baseline ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({1, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({1, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU 1024B: ReadOnly 2t (pure Get throughput, no eviction) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUSN, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC 1024B: ReadOnly 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- LRU CN 1024B: Partitioned + ReadOnly 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUCN, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUCN, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Remote-Only 1024B: Partitioned + ReadOnly (thread 1 = 100% remote) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallRemote, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRemote, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRURemote, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRURemote, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
struct NUMABench_CacheLib : NUMABench<CacheLibAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_CacheLib, Run)(benchmark::State& state) {
    RunNUMABench<CacheLibAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({1, 256, 0, 64, 100000})
    ->Args({1, 256, 1, 64, 100000})
    ->Args({2, 256, 0, 64, 100000})
    ->Args({2, 256, 1, 64, 100000})
    ->Args({2, 256, 2, 64, 100000})
    ->Args({2, 64, 0, 64, 100000})
    ->Args({2, 64, 1, 64, 100000})
    ->Args({2, 64, 2, 64, 100000})
    ->Args({4, 256, 0, 64, 100000})
    ->Args({4, 256, 1, 64, 100000})
    ->Args({4, 256, 2, 64, 100000})
    ->Args({4, 64, 0, 64, 100000})
    ->Args({4, 64, 1, 64, 100000})
    ->Args({4, 64, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- CacheLib NUMA-aware (per-pool setMemBind) ---

struct NUMABench_CacheLibNuma : NUMABench<CacheLibNumaAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_CacheLibNuma, Run)(benchmark::State& state) {
    RunNUMABench<CacheLibNumaAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({1, 256, 0, 64, 100000})
    ->Args({1, 256, 1, 64, 100000})
    ->Args({2, 256, 0, 64, 100000})
    ->Args({2, 256, 1, 64, 100000})
    ->Args({2, 256, 2, 64, 100000})
    ->Args({2, 64, 0, 64, 100000})
    ->Args({2, 64, 1, 64, 100000})
    ->Args({2, 64, 2, 64, 100000})
    ->Args({4, 256, 0, 64, 100000})
    ->Args({4, 256, 1, 64, 100000})
    ->Args({4, 256, 2, 64, 100000})
    ->Args({4, 64, 0, 64, 100000})
    ->Args({4, 64, 1, 64, 100000})
    ->Args({4, 64, 2, 64, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// ============================================================================
//  Equal-capacity CacheLib benchmarks (inside USE_CACHELIB guard)
//  CacheLib: pagesPerNode=8192 → 8192*4KB = 32MB (already at minimum floor)
// ============================================================================
#ifdef USE_CACHELIB

// --- Equal cap: Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Trace 2t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Partitioned 2t, large values ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Partitioned 4t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Shared 4t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Single-thread ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: CacheLibNuma Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: CacheLibNuma Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: CacheLibNuma Single-thread ---
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap 1024B: CacheLib ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({1, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap 1024B: CacheLibNuma ---
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 32768, 1, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap 1024B: CacheLib ReadOnly ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Working-set sweep: CacheLib UniformRO 64MB/128MB total ---
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 65536, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({2, 131072, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 65536, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({2, 131072, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#endif

// ============================================================================
//  NUMA-dominant workloads: larger values to make remote memory cost dominate
//  With 1KB values, a remote memcpy (~200ns) dwarfs lock overhead (~50ns)
// ============================================================================

// --- FurrBall TL, large values ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 512, 0, 256, 100000})
    ->Args({2, 512, 0, 1024, 100000})
    ->Args({4, 512, 0, 256, 100000})
    ->Args({4, 512, 0, 1024, 100000})
    ->Args({2, 8192, 3, 64, 100000})
    ->Args({2, 8192, 3, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall SN, large values ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 256, 0, 256, 100000})
    ->Args({2, 256, 0, 1024, 100000})
    ->Args({4, 256, 0, 256, 100000})
    ->Args({4, 256, 0, 1024, 100000})
    ->Args({2, 4096, 3, 64, 100000})
    ->Args({2, 4096, 3, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall CN, large values ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 512, 0, 256, 100000})
    ->Args({2, 512, 0, 1024, 100000})
    ->Args({2, 8192, 3, 64, 100000})
    ->Args({2, 8192, 3, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- TBB, large values ---
BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 256, 0, 256, 100000})
    ->Args({2, 256, 0, 1024, 100000})
    ->Args({4, 256, 0, 256, 100000})
    ->Args({4, 256, 0, 1024, 100000})
    ->Args({2, 4096, 3, 64, 100000})
    ->Args({2, 4096, 3, 1024, 100000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Equal-capacity benchmarks: 32MB budget for all systems
//  All systems: totalCapacityKB=32768 (32MB total, auto-split across nodes)
//  Universe=700000 (~2x oversubscription at 64B values, ~320K keys fit)
// ============================================================================

// --- Equal cap: Partitioned 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Shared 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Trace 2t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 32768, 2, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Partitioned 2t, large values (NUMA-dominant) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 32768, 0, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: ReadOnly 2t, large values ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({2, 32768, 3, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Partitioned 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({4, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Shared 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({4, 32768, 1, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- Equal cap: Single-thread (baseline, 1 node) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({1, 32768, 0, 64, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  Working-set sweep: 32MB/64MB/128MB total at 1024B
//  Uses UniformRO (workload=4) to spread access across all keys uniformly.
//  Zipfian concentrates on ~30K hot keys regardless of capacity, so
//  the hot set fits in L3. Uniform forces all allocated keys to be accessed,
//  exceeding L3 at 64MB+.
//
//  L3 cache on Ice Lake 8375C is 56MB shared per socket.
//  TL/SN/Remote at each total size reveals the L3-to-DRAM crossover.
//
//  Also includes 32MB UniformRO baseline for comparison with Zipfian ReadOnly.
// ============================================================================

// --- 32MB UniformRO baseline (compare with Zipfian ReadOnly at 32MB) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 32768, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 32768, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRemote, Run)
    ->Args({2, 32768, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- 64MB total: just above L3 ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 65536, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 65536, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRemote, Run)
    ->Args({2, 65536, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- 128MB total: well above L3 ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({2, 131072, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({2, 131072, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallRemote, Run)
    ->Args({2, 131072, 4, 1024, 700000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
//  64MB cache / 2M universe — ARC vs LRU eviction pressure
//  2M keys × 64B = 128MB working set, 64MB cache → ~50% miss rate
//  Well above per-CCD L3 (32MB on Milan), eviction policy matters here.
// ============================================================================

// --- ARC vs LRU: Partitioned 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC vs LRU: Shared 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 65536, 1, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 65536, 1, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC vs LRU: Zipfian ReadOnly 4t (pure read, no write path drain) ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 65536, 3, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 65536, 3, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC vs LRU: Trace 4t ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 65536, 2, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 65536, 2, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- ARC vs LRU: 1T baseline ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({1, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({1, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- TBB + CacheLib baselines at 64MB/2M ---
BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({4, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

#ifdef USE_CACHELIB
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({4, 65536, 0, 64, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

// --- 1024B values: 2M keys × 1KB = 2GB working set, 64MB cache = 3.1% hit rate ---
BENCHMARK_REGISTER_F(NUMABench_FurrBallTL, Run)
    ->Args({4, 65536, 0, 1024, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(NUMABench_FurrBallLRUTL, Run)
    ->Args({4, 65536, 0, 1024, 2000000})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    // Clean up FurrBall global state
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
