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
    double p99GetNs = 0;
    double p50SetNs = 0;
    double p99SetNs = 0;
    double opsPerSec = 0;

    // Per-node p50 (for local vs remote analysis)
    std::vector<double> perNodeP50Get;
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
        ar.p99GetNs = (double)allGet[allGet.size() * 99 / 100];
    }
    if (!allSet.empty()) {
        std::sort(allSet.begin(), allSet.end());
        ar.p50SetNs = (double)allSet[allSet.size() / 2];
        ar.p99SetNs = (double)allSet[allSet.size() * 99 / 100];
    }

    ar.perNodeP50Get.resize(maxNode + 1, 0);
    ar.perNodeP99Get.resize(maxNode + 1, 0);
    for (const auto& r : results) {
        if (r.numaNode >= 0 && !r.getLatencies.empty()) {
            auto lats = r.getLatencies;
            std::sort(lats.begin(), lats.end());
            ar.perNodeP50Get[r.numaNode] = (double)lats[lats.size() / 2];
            ar.perNodeP99Get[r.numaNode] = (double)lats[lats.size() * 99 / 100];
        }
    }

    return ar;
}

static void reportAgg(benchmark::State& state, const AggregateResult& ar) {
    state.counters["hit_rate_pct"] = ar.hitRatePct;
    state.counters["p50_get_ns"] = ar.p50GetNs;
    state.counters["p99_get_ns"] = ar.p99GetNs;
    state.counters["p50_set_ns"] = ar.p50SetNs;
    state.counters["p99_set_ns"] = ar.p99SetNs;
    state.counters["ops_per_sec"] = ar.opsPerSec;
    for (size_t n = 0; n < ar.perNodeP50Get.size(); n++) {
        std::string p50name = "node" + std::to_string(n) + "_p50_get_ns";
        std::string p99name = "node" + std::to_string(n) + "_p99_get_ns";
        state.counters[p50name] = ar.perNodeP50Get[n];
        state.counters[p99name] = ar.perNodeP99Get[n];
    }
}

// ============================================================================
//  Workload generators
//
//  Each generates a sequence of (key_string, should_insert) operations.
//  Partitioned: thread T gets keys [T * rangeSize, (T+1) * rangeSize)
//  Shared: all threads use same key space
//  Trace: replay Twitter trace across all threads (interleaved)
// ============================================================================

enum class WorkloadType { Partitioned, Shared, Trace };

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

template <Routing R>
struct FurrBallAdapter {
    static constexpr const char* Name = (R == Routing::ThreadLocal)
        ? "FurrBall_TL" : "FurrBall_RR";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<ArcPolicy>* fb = nullptr;
    std::string fbPath;
    int nodeCount = 1;
    int pageCount = 64;
    static int runId;

    void create(int nodeCount, int pagesPerNode) {
        this->nodeCount = nodeCount;
        pageCount = pagesPerNode;

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
        fc.InitialPageCount = pagesPerNode;
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

    int numNodes() const { return nodeCount; }
};
template <Routing R> int FurrBallAdapter<R>::runId = 0;

// --- FurrBall Single Node (no NUMA baseline) ---

struct FurrBallSNAdapter {
    static constexpr const char* Name = "FurrBall_SN";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<ArcPolicy>* fb = nullptr;
    std::string fbPath;
    static int runId;

    void create(int nodeCount, int pagesPerNode) {
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

        fbPath = "/tmp/numabench_sn_" + std::to_string(runId++);

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.InitialPageCount = pagesPerNode;
        fc.IsVolatile = true;
        fc.EnableNUMA = false;
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

    static int numNodes() { return 1; }
};

int FurrBallSNAdapter::runId = 0;

// --- FurrBall cross-node adapter (all data on node 0, threads on node 0+1) ---
// Isolates pure NUMA memory latency: every access from thread 1 is remote.

struct FurrBallCNAdapter {
    static constexpr const char* Name = "FurrBall_CN";

    static constexpr size_t PAGE_SIZE = 4096;

    FurrBall<ArcPolicy>* fb = nullptr;
    std::string fbPath;
    static int runId;

    void create(int nodeCount, int pagesPerNode) {
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

        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = false;

        FurrConfig fc;
        fc.PageSize = PAGE_SIZE;
        fc.InitialPageCount = pagesPerNode;
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

// --- TBB concurrent_hash_map adapter ---

struct TBBAdapter {
    static constexpr const char* Name = "TBB";

    using Map = tbb::concurrent_hash_map<std::string, std::vector<uint8_t>>;
    Map* map = nullptr;

    void create(int, int) {
        map = new Map();
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

    std::unique_ptr<LruAllocator> cache;
    PoolId pool;
    size_t cacheSizeBytes;
    static int runId;
    std::string cacheDir;

    void create(int, int pagesPerNode) {
        cacheSizeBytes = (size_t)pagesPerNode * PAGE_SIZE;
        if (cacheSizeBytes < 32 * 1024 * 1024) cacheSizeBytes = 32 * 1024 * 1024;

        cacheDir = "/tmp/cachelib_numabench_" + std::to_string(runId++);
        mkdir(cacheDir.c_str(), 0755);

        LruAllocator::Config config;
        config.setCacheName("numabench_cl");
        config.setCacheSize(cacheSizeBytes);
        config.cacheDir = cacheDir;

        std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024, 2048};
        config.setDefaultAllocSizes(allocSizes);

        cache = std::make_unique<LruAllocator>(config);
        pool = cache->addPool("default", cacheSizeBytes / 2);
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

// --- cachelib NUMA-aware adapter (per-pool setMemBind) ---
// Two pools, each bound to a NUMA node. Uses thread_local pool routing.

#include <cachelib/allocator/CacheAllocator.h>

struct CacheLibNumaAdapter {
    static constexpr const char* Name = "CacheLib_NUMA";

    using LruAllocator = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;

    static constexpr size_t PAGE_SIZE = 4096;

    std::unique_ptr<LruAllocator> cache;
    PoolId pools[2];
    size_t cacheSizeBytes;
    static int runId;
    std::string cacheDir;
    int nodeCount;
    static inline thread_local int currentPoolIdx = 0;

    void create(int nodeCount, int pagesPerNode) {
        this->nodeCount = nodeCount;
        cacheSizeBytes = (size_t)pagesPerNode * PAGE_SIZE;
        size_t perNodeBytes = cacheSizeBytes / std::max(nodeCount, 1);
        if (perNodeBytes < 32 * 1024 * 1024) perNodeBytes = 32 * 1024 * 1024;

        cacheDir = "/tmp/cachelib_numa_numabench_" + std::to_string(runId++);
        mkdir(cacheDir.c_str(), 0755);

        LruAllocator::Config config;
        config.setCacheName("numabench_cl_numa");
        config.setCacheSize(perNodeBytes * nodeCount);
        config.cacheDir = cacheDir;

        std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024, 2048};
        config.setDefaultAllocSizes(allocSizes);

        cache = std::make_unique<LruAllocator>(config);

        if (nodeCount >= 2) {
            pools[0] = cache->addPool("node0", perNodeBytes / 2);
            pools[1] = cache->addPool("node1", perNodeBytes / 2);
        } else {
            pools[0] = cache->addPool("node0", perNodeBytes / 2);
            pools[1] = pools[0];
        }
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
        if (!cacheDir.empty()) {
            std::string rm = "rm -rf " + cacheDir;
            system(rm.c_str());
        }
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
    int numaNode)
{
    ThreadResult result;
    result.numaNode = numaNode;

    if (numaNode >= 0) {
        Numatic::PinCurrentThreadToNode(numaNode);
    }

    uint8_t outBuf[4096];
    uint8_t val[64];
    memset(val, 'x', 64);

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
            } else {
                result.misses++;
                auto ts0 = std::chrono::high_resolution_clock::now();
                sys.put(op.key, val, 64);
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
//   state.range(1) = pagesPerNode
//   state.range(2) = workload type (0=Partitioned, 1=Shared, 2=Trace)

static const char* workloadName(int w) {
    switch (w) {
    case 0: return "Partitioned";
    case 1: return "Shared";
    case 2: return "Trace";
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
    int pagesPerNode = state.range(1);
    int workloadType = state.range(2);

    for (auto _ : state) {
        state.PauseTiming();

        System sys;
        sys.create(numThreads, pagesPerNode);

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
                cfg.zipfianUniverse = 100000;
                cfg.zipfianTheta = 0.99;
                cfg.threadIndex = t;
                cfg.totalThreads = numThreads;
                cfg.trace = &trace;

                int nn = sys.numNodes();
                results[t] = runWorker(sys, cfg, nn > 0 ? (t % nn) : -1);
            });
        }

        for (auto& th : threads) th.join();

        state.PauseTiming();

        auto wallEnd = std::chrono::high_resolution_clock::now();
        double elapsedSec = std::chrono::duration<double>(wallEnd - wallStart).count();

        AggregateResult ar = aggregate(results, elapsedSec);
        reportAgg(state, ar);

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
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall RoundRobin ---

struct NUMABench_FurrBallRR : NUMABench<FurrBallAdapter<Routing::RoundRobin>> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallRR, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallAdapter<Routing::RoundRobin>>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallRR, Run)
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall Single Node (no NUMA baseline) ---

struct NUMABench_FurrBallSN : NUMABench<FurrBallSNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallSN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallSNAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallSN, Run)
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- FurrBall Cross Node (all data on node 0, threads on 0+1) ---

struct NUMABench_FurrBallCN : NUMABench<FurrBallCNAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_FurrBallCN, Run)(benchmark::State& state) {
    RunNUMABench<FurrBallCNAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_FurrBallCN, Run)
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- TBB concurrent_hash_map ---

struct NUMABench_TBB : NUMABench<TBBAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_TBB, Run)(benchmark::State& state) {
    RunNUMABench<TBBAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_TBB, Run)
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- CacheLib (default, accidental interleaving) ---

#ifdef USE_CACHELIB
struct NUMABench_CacheLib : NUMABench<CacheLibAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_CacheLib, Run)(benchmark::State& state) {
    RunNUMABench<CacheLibAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_CacheLib, Run)
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);

// --- CacheLib NUMA-aware (per-pool setMemBind) ---

struct NUMABench_CacheLibNuma : NUMABench<CacheLibNumaAdapter> {};
BENCHMARK_DEFINE_F(NUMABench_CacheLibNuma, Run)(benchmark::State& state) {
    RunNUMABench<CacheLibNumaAdapter>(state, trace);
}
BENCHMARK_REGISTER_F(NUMABench_CacheLibNuma, Run)
    ->Args({1, 64, 0})
    ->Args({1, 64, 1})
    ->Args({2, 64, 0})
    ->Args({2, 64, 1})
    ->Args({2, 64, 2})
    ->Args({2, 16, 0})
    ->Args({2, 16, 1})
    ->Args({2, 16, 2})
    ->Args({4, 64, 0})
    ->Args({4, 64, 1})
    ->Args({4, 64, 2})
    ->Args({4, 16, 0})
    ->Args({4, 16, 1})
    ->Args({4, 16, 2})
    ->Iterations(10)
    ->Unit(benchmark::kMicrosecond);
#endif

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
