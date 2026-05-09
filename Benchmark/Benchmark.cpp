#include "Furrballs.h"
#include "Numatic.h"
#include "Policy.h"
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <cmath>
#include <random>

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;

static constexpr int ITERATIONS = 5;
static constexpr size_t PAGE_SIZE = 4096;
static constexpr size_t VAL_SIZE = 64;
static constexpr size_t KEYS_PER_PAGE = PAGE_SIZE / VAL_SIZE;

static constexpr size_t SMALL_PAGES = 16;
static constexpr size_t SMALL_HOT_KEYS = (SMALL_PAGES - 1) * KEYS_PER_PAGE;

static constexpr size_t ZIPF_UNIVERSE = 10000;
static constexpr size_t LOOP_UNIVERSE = 2000;
static constexpr size_t TEMPORAL_UNIVERSE = 10000;
static constexpr size_t TEMPORAL_PERIOD = 4;
static constexpr size_t SCAN_HOT = 1000;
static constexpr size_t SCAN_UNIVERSE = 10000;

static constexpr size_t READ_OPS = 100000;
static constexpr size_t DESIRE_UPDATE_INTERVAL = 500;

static double computePercentile(std::vector<ns>& v, double pct) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[static_cast<size_t>(pct * v.size() / 100.0)].count();
}

static std::vector<uint64_t> genZipfian(size_t n, size_t universe, double theta, std::mt19937_64& rng) {
    double alpha = 1.0 / (1.0 - theta);
    double zeta = 0.0;
    for (size_t i = 0; i < universe; i++) zeta += 1.0 / std::pow((double)(i + 1), theta);
    double eta = (1.0 - std::pow(2.0 / (double)universe, 1.0 - theta)) /
                 (1.0 - zeta + std::pow(2.0 / (double)universe, 1.0 - theta) / (1.0 - 1.0 / (double)universe));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        double u = dist(rng);
        double uz = u * zeta;
        if (uz < 1.0) { result[i] = 0; continue; }
        if (uz < 1.0 + std::pow(0.5, theta)) { result[i] = 1; continue; }
        result[i] = (size_t)((double)universe * std::pow(eta * u - eta + 1.0, 1.0 / (1.0 - theta))) % universe;
    }
    return result;
}

static std::vector<uint64_t> genLooping(size_t n, size_t loopSize) {
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) result[i] = i % loopSize;
    return result;
}

static std::vector<uint64_t> genTemporalShift(size_t n, size_t universe, size_t period) {
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        size_t base = (i / period) % universe;
        size_t off = i % period;
        result[i] = (base + off * 137) % universe;
    }
    return result;
}

static std::vector<uint64_t> genScanResistant(size_t n, size_t hotSet, size_t universe, std::mt19937_64& rng) {
    std::uniform_int_distribution<uint64_t> hotDist(0, hotSet - 1);
    std::uniform_int_distribution<uint64_t> coldDist(hotSet, universe - 1);
    std::bernoulli_distribution hotProb(0.9);
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        result[i] = hotProb(rng) ? hotDist(rng) : coldDist(rng);
    }
    return result;
}

template<typename Policy>
static FurrBall<Policy>* createSmallFB(const std::string& dbPath, bool threadLocal = false) {
    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    nc.UseThreadLocalRouting = threadLocal;
    FurrConfig fc;
    fc.EnableLogging = false;
    fc.EnableNUMA = true;
    fc.PageSize = PAGE_SIZE;
    fc.InitialPageCount = SMALL_PAGES;
    fc.numaConfig = &nc;
    return FurrBall<Policy>::CreateBall(dbPath, fc, true);
}

template<typename Policy>
static FurrBall<Policy>* createLargeFB(const std::string& dbPath, bool threadLocal = false) {
    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    nc.UseThreadLocalRouting = threadLocal;
    FurrConfig fc;
    fc.EnableLogging = false;
    fc.EnableNUMA = true;
    fc.PageSize = PAGE_SIZE;
    fc.InitialPageCount = 2048;
    fc.numaConfig = &nc;
    return FurrBall<Policy>::CreateBall(dbPath, fc, true);
}

// =====================================================================
//  Architecture sections
// =====================================================================

template<typename Policy>
static void runA1_PureNUMA(const char* name, int numNodes) {
    if (numNodes < 2) return;
    using FB = FurrBall<Policy>;

    FB* fb = createLargeFB<Policy>(std::string("Bench_") + name + "_A1");
    if (!fb) return;

    std::cout << std::string(100, '-') << std::endl;
    std::cout << "A1: Pure NUMA Cross-Node Overhead (" << name << ")" << std::endl;
    std::cout << std::string(100, '-') << std::endl;

    std::vector<char> val(VAL_SIZE, 'X');
    std::vector<char> out(VAL_SIZE + 64);
    size_t ops = 10000;

    fb->Set("numa_key", val.data(), VAL_SIZE);

    std::vector<ns> localLat, crossLat;
    localLat.resize(ops);
    crossLat.resize(ops);

    for (size_t i = 0; i < ops; i++) {
        size_t os = 0;
        auto t0 = Clock::now();
        fb->Get("numa_key", out.data(), out.size(), os);
        localLat[i] = Clock::now() - t0;
    }

    std::thread crossThread([&]() {
        Numatic::PinCurrentThreadToNode(1);
        for (size_t i = 0; i < ops; i++) {
            size_t os = 0;
            auto t0 = Clock::now();
            fb->Get("numa_key", out.data(), out.size(), os);
            crossLat[i] = Clock::now() - t0;
        }
    });
    crossThread.join();

    double lp50 = computePercentile(localLat, 50);
    double cp50 = computePercentile(crossLat, 50);
    double lp99 = computePercentile(localLat, 99);
    double cp99 = computePercentile(crossLat, 99);
    double oh50 = (lp50 > 0) ? (cp50 - lp50) / lp50 * 100.0 : 0;
    double oh99 = (lp99 > 0) ? (cp99 - lp99) / lp99 * 100.0 : 0;

    std::cout << "  Local  Get p50: " << std::fixed << std::setprecision(0) << lp50 << " ns  p99: " << lp99 << " ns" << std::endl;
    std::cout << "  Cross  Get p50: " << std::fixed << std::setprecision(0) << cp50 << " ns  p99: " << cp99 << " ns" << std::endl;
    std::cout << "  Cross-node overhead: p50 " << std::fixed << std::setprecision(1) << oh50 << "%  p99 " << oh99 << "%" << std::endl;

    delete fb;
}

template<typename Policy>
static void runA2_Routing(const char* name, int numNodes) {
    if (numNodes < 2) return;
    using FB = FurrBall<Policy>;

    std::cout << std::string(100, '-') << std::endl;
    std::cout << "A2: Routing Strategy (" << name << ", 5000 keys/thread)" << std::endl;
    std::cout << std::string(100, '-') << std::endl;

    size_t kpt = 5000;
    auto runRouting = [&](FB* fb, const std::string& strat) {
        std::vector<ns> selfGets, crossGets;
        for (int iter = 0; iter < ITERATIONS; iter++) {
            std::vector<std::thread> threads(numNodes);
            struct TR { std::vector<ns> self, cross; };
            std::vector<TR> results(numNodes);
            std::vector<char> val(VAL_SIZE, 'X'), out(VAL_SIZE + 64);

            for (int t = 0; t < numNodes; t++) {
                threads[t] = std::thread([&, t]() {
                    Numatic::PinCurrentThreadToNode(t);
                    for (size_t i = 0; i < kpt; i++)
                        fb->Set("rt_" + std::to_string(t) + "_" + std::to_string(i), val.data(), VAL_SIZE);
                    results[t].self.resize(kpt);
                    results[t].cross.resize(kpt);
                    int other = (t + 1) % numNodes;
                    for (size_t i = 0; i < kpt; i++) {
                        size_t os = 0;
                        auto t0 = Clock::now();
                        fb->Get("rt_" + std::to_string(t) + "_" + std::to_string(i), out.data(), out.size(), os);
                        results[t].self[i] = Clock::now() - t0;
                    }
                    for (size_t i = 0; i < kpt; i++) {
                        size_t os = 0;
                        auto t0 = Clock::now();
                        fb->Get("rt_" + std::to_string(other) + "_" + std::to_string(i), out.data(), out.size(), os);
                        results[t].cross[i] = Clock::now() - t0;
                    }
                });
            }
            for (auto& th : threads) th.join();
            for (auto& r : results) {
                selfGets.insert(selfGets.end(), r.self.begin(), r.self.end());
                crossGets.insert(crossGets.end(), r.cross.begin(), r.cross.end());
            }
        }
        double sp50 = computePercentile(selfGets, 50);
        double cp50 = computePercentile(crossGets, 50);
        double optRate = (fb->Stats.GetHitCount() > 0)
            ? 100.0 * fb->Stats.GetLocalHitCount() / fb->Stats.GetHitCount() : 0;
        std::cout << "  " << std::left << std::setw(15) << strat << " | "
                  << "Self-Get p50 " << std::setw(7) << std::fixed << std::setprecision(0) << sp50 << " ns | "
                  << "Cross-Get p50 " << std::setw(7) << cp50 << " ns | "
                  << "Local-hit " << std::setprecision(1) << optRate << "%" << std::endl;
    };

    FB* fbRR = createLargeFB<Policy>(std::string("Bench_") + name + "_A2_RR", false);
    if (fbRR) { runRouting(fbRR, "Round-robin"); delete fbRR; }
    FB* fbTL = createLargeFB<Policy>(std::string("Bench_") + name + "_A2_TL", true);
    if (fbTL) { runRouting(fbTL, "Thread-local"); delete fbTL; }
}

template<typename Policy>
static void runA3_Scaling(const char* name, int numNodes) {
    if (numNodes < 2) return;
    using FB = FurrBall<Policy>;

    std::cout << std::string(100, '-') << std::endl;
    std::cout << "A3: Thread Scaling (" << name << ", thread-local, 64B)" << std::endl;
    std::cout << std::string(100, '-') << std::endl;
    std::cout << "  Threads | Set ops/s | Get ops/s" << std::endl;
    std::cout << "  --------|----------|----------" << std::endl;

    for (int tc : {4, 8, 16, 32, 64}) {
        int kpt = (tc <= 16) ? 2500 : 1000;
        double sSum = 0, gSum = 0;
        int valid = 0;
        for (int it = 0; it < ITERATIONS; it++) {
            NumaConfig nc; nc.UseThreadLocalRouting = true;
            FurrConfig fc; fc.EnableLogging = false; fc.EnableNUMA = true;
            fc.PageSize = PAGE_SIZE; fc.InitialPageCount = 2048; fc.numaConfig = &nc;
            FB* sfb = FB::CreateBall(
                std::string("Bench_") + name + "_A3_" + std::to_string(tc) + "_" + std::to_string(it), fc, true);
            if (!sfb) continue;
            valid++;

            auto benchMT = [&](bool doSet) -> double {
                std::vector<std::vector<ns>> lats(tc);
                std::vector<std::thread> threads(tc);
                for (int t = 0; t < tc; t++) {
                    threads[t] = std::thread([&, t]() {
                        Numatic::PinCurrentThreadToNode(t % numNodes);
                        std::vector<char> val(VAL_SIZE, 'X'), out(VAL_SIZE + 64);
                        lats[t].resize(kpt);
                        for (size_t i = 0; i < kpt; i++) {
                            std::string k = "sc_" + std::to_string(t) + "_" + std::to_string(i);
                            if (doSet) {
                                auto t0 = Clock::now();
                                sfb->Set(k, val.data(), VAL_SIZE);
                                lats[t][i] = Clock::now() - t0;
                            } else {
                                size_t os = 0;
                                auto t0 = Clock::now();
                                sfb->Get(k, out.data(), out.size(), os);
                                lats[t][i] = Clock::now() - t0;
                            }
                        }
                    });
                }
                auto t0 = Clock::now();
                for (auto& th : threads) th.join();
                auto t1 = Clock::now();
                size_t totalOps = tc * kpt;
                return totalOps / std::chrono::duration<double>(t1 - t0).count();
            };

            sSum += benchMT(true);
            gSum += benchMT(false);
            delete sfb;
        }
        if (valid > 0) {
            std::cout << "  " << std::setw(7) << tc << " | "
                      << std::fixed << std::setprecision(0) << std::setw(8) << sSum / valid << " | "
                      << std::setw(8) << gSum / valid << std::endl;
        }
    }
}

// =====================================================================
//  Policy sections (eviction pressure)
// =====================================================================

template<typename Policy>
struct PolicyResult {
    double hitRate = 0;
    double localHitRate = 0;
    size_t hits = 0;
    size_t misses = 0;
    size_t evictions = 0;
    size_t migrations = 0;
    size_t frozenHits = 0;
    double getP50 = 0;
    double getP99 = 0;
};

template<typename Policy>
static PolicyResult<Policy> runEvictionWorkload(
    const std::string& dbName,
    const std::vector<uint64_t>& reads,
    int numNodes,
    bool threadLocal)
{
    using FB = FurrBall<Policy>;
    PolicyResult<Policy> result;

    FB* fb = createSmallFB<Policy>(dbName, threadLocal);
    if (!fb) return result;

    size_t maxKey = 0;
    for (auto k : reads) if (k > maxKey) maxKey = k;
    std::vector<char> val(VAL_SIZE, 'X');
    std::vector<char> out(VAL_SIZE + 64);

    size_t populateKeys = maxKey + 1;
    for (size_t i = 0; i < populateKeys; i++) {
        fb->Set("ev_" + std::to_string(i), val.data(), VAL_SIZE);
    }

    for (int n = 0; n < numNodes; n++) fb->UpdateMinDesire(n);

    unsigned int hitsBefore = fb->Stats.GetHitCount();
    unsigned int missesBefore = fb->Stats.GetMissCount();
    unsigned int evictBefore = fb->Stats.GetEvictionCount();
    unsigned int migrateBefore = fb->Stats.GetMigrationCount();
    unsigned int frozenBefore = fb->Stats.GetFrozenPageHits();

    std::vector<ns> getLats(reads.size());
    size_t desireCounter = 0;

    for (size_t i = 0; i < reads.size(); i++) {
        std::string key = "ev_" + std::to_string(reads[i]);
        size_t os = 0;
        auto t0 = Clock::now();
        fb->Get(key, out.data(), out.size(), os);
        getLats[i] = Clock::now() - t0;

        desireCounter++;
        if (desireCounter >= DESIRE_UPDATE_INTERVAL) {
            for (int n = 0; n < numNodes; n++) fb->UpdateMinDesire(n);
            desireCounter = 0;
        }
    }

    result.hits = fb->Stats.GetHitCount() - hitsBefore;
    result.misses = fb->Stats.GetMissCount() - missesBefore;
    result.evictions = fb->Stats.GetEvictionCount() - evictBefore;
    result.migrations = fb->Stats.GetMigrationCount() - migrateBefore;
    result.frozenHits = fb->Stats.GetFrozenPageHits() - frozenBefore;

    size_t total = result.hits + result.misses;
    result.hitRate = (total > 0) ? 100.0 * result.hits / total : 0;
    result.localHitRate = (result.hits > 0)
        ? 100.0 * (result.hits - result.frozenHits) * 
          (fb->Stats.GetLocalHitCount() > 0 ? 1.0 : 1.0) / result.hits : 0;
    result.getP50 = computePercentile(getLats, 50);
    result.getP99 = computePercentile(getLats, 99);

    delete fb;
    return result;
}

template<typename Policy>
static PolicyResult<Policy> runMultiNodeWorkload(
    const std::string& dbName,
    const std::vector<uint64_t>& reads,
    int numNodes)
{
    using FB = FurrBall<Policy>;
    PolicyResult<Policy> result;

    FB* fb = createSmallFB<Policy>(dbName, true);
    if (!fb) return result;

    size_t maxKey = 0;
    for (auto k : reads) if (k > maxKey) maxKey = k;
    std::vector<char> val(VAL_SIZE, 'X');
    std::vector<char> out(VAL_SIZE + 64);

    size_t populateKeys = maxKey + 1;
    size_t keysPerNode = (populateKeys + numNodes - 1) / numNodes;

    std::vector<std::thread> popThreads(numNodes);
    for (int n = 0; n < numNodes; n++) {
        popThreads[n] = std::thread([&, n]() {
            Numatic::PinCurrentThreadToNode(n);
            for (size_t i = 0; i < keysPerNode; i++) {
                size_t k = n * keysPerNode + i;
                if (k < populateKeys) {
                    fb->Set("mn_" + std::to_string(k), val.data(), VAL_SIZE);
                }
            }
        });
    }
    for (auto& th : popThreads) th.join();

    for (int n = 0; n < numNodes; n++) fb->UpdateMinDesire(n);

    unsigned int hitsBefore = fb->Stats.GetHitCount();
    unsigned int missesBefore = fb->Stats.GetMissCount();
    unsigned int evictBefore = fb->Stats.GetEvictionCount();
    unsigned int migrateBefore = fb->Stats.GetMigrationCount();
    unsigned int frozenBefore = fb->Stats.GetFrozenPageHits();
    unsigned int localHitsBefore = fb->Stats.GetLocalHitCount();

    size_t readsPerNode = reads.size() / numNodes;
    std::vector<std::vector<ns>> nodeLats(numNodes);
    std::vector<size_t> nodeDesireCount(numNodes, 0);

    std::vector<std::thread> readThreads(numNodes);
    for (int n = 0; n < numNodes; n++) {
        readThreads[n] = std::thread([&, n]() {
            Numatic::PinCurrentThreadToNode(n);
            std::mt19937_64 rng(n * 42 + 7);
            nodeLats[n].resize(readsPerNode);
            for (size_t i = 0; i < readsPerNode; i++) {
                size_t idx = rng() % reads.size();
                std::string key = "mn_" + std::to_string(reads[idx]);
                size_t os = 0;
                auto t0 = Clock::now();
                fb->Get(key, out.data(), out.size(), os);
                nodeLats[n][i] = Clock::now() - t0;

                nodeDesireCount[n]++;
                if (nodeDesireCount[n] >= DESIRE_UPDATE_INTERVAL) {
                    for (int nn = 0; nn < numNodes; nn++) fb->UpdateMinDesire(nn);
                    nodeDesireCount[n] = 0;
                }
            }
        });
    }
    for (auto& th : readThreads) th.join();

    result.hits = fb->Stats.GetHitCount() - hitsBefore;
    result.misses = fb->Stats.GetMissCount() - missesBefore;
    result.evictions = fb->Stats.GetEvictionCount() - evictBefore;
    result.migrations = fb->Stats.GetMigrationCount() - migrateBefore;
    result.frozenHits = fb->Stats.GetFrozenPageHits() - frozenBefore;

    size_t total = result.hits + result.misses;
    result.hitRate = (total > 0) ? 100.0 * result.hits / total : 0;
    unsigned int localHits = fb->Stats.GetLocalHitCount() - localHitsBefore;
    result.localHitRate = (result.hits > 0) ? 100.0 * localHits / result.hits : 0;

    std::vector<ns> allLats;
    for (auto& v : nodeLats) allLats.insert(allLats.end(), v.begin(), v.end());
    result.getP50 = computePercentile(allLats, 50);
    result.getP99 = computePercentile(allLats, 99);

    delete fb;
    return result;
}

static void printPolicyResult(const char* name, const auto& r) {
    std::cout << "  " << std::left << std::setw(12) << name << " | "
              << std::right << std::fixed << std::setprecision(1) << std::setw(6) << r.hitRate << "% HR | "
              << std::setw(6) << r.localHitRate << "% LHR | "
              << std::setw(6) << r.migrations << " mig | "
              << std::setw(6) << r.evictions << " ev | "
              << std::setw(5) << r.frozenHits << " fz | "
              << std::setw(6) << std::setprecision(0) << r.getP50 << " ns p50"
              << std::endl;
}

// =====================================================================
//  Main
// =====================================================================

template<typename P1, typename P2, typename P3>
void runFullBenchmark(const char* n1, const char* n2, const char* n3, int numNodes) {
    std::mt19937_64 rng(42);

    // === ARCHITECTURE ===
    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "ARCHITECTURE VALIDATION" << std::endl;
    std::cout << std::string(100, '=') << std::endl;

    runA1_PureNUMA<P1>(n1, numNodes);
    runA2_Routing<P1>(n1, numNodes);
    runA3_Scaling<P1>(n1, numNodes);

    // === POLICY: Single-Node Eviction ===
    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "POLICY: Eviction Pressure (" << SMALL_PAGES << " pages, "
              << SMALL_HOT_KEYS << " hot keys, " << ZIPF_UNIVERSE << " universe, ~"
              << std::fixed << std::setprecision(0)
              << 100.0 * SMALL_HOT_KEYS / ZIPF_UNIVERSE << "% cap)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;

    auto wlZipf = genZipfian(READ_OPS, ZIPF_UNIVERSE, 0.99, rng);
    rng.seed(42);
    auto wlScan = genScanResistant(READ_OPS, SCAN_HOT, SCAN_UNIVERSE, rng);
    auto wlLoop = genLooping(READ_OPS, LOOP_UNIVERSE);
    auto wlTemp = genTemporalShift(READ_OPS, TEMPORAL_UNIVERSE, TEMPORAL_PERIOD);

    auto runWorkload = [&](const char* section, const std::vector<uint64_t>& reads, int nodes) {
        std::cout << "\n--- " << section << " ---" << std::endl;
        if (nodes >= 2) {
            auto r1 = runMultiNodeWorkload<P1>(std::string("BP_") + section + "_" + n1, reads, nodes);
            auto r2 = runMultiNodeWorkload<P2>(std::string("BP_") + section + "_" + n2, reads, nodes);
            auto r3 = runMultiNodeWorkload<P3>(std::string("BP_") + section + "_" + n3, reads, nodes);
            printPolicyResult(n1, r1);
            printPolicyResult(n2, r2);
            printPolicyResult(n3, r3);
        } else {
            auto r1 = runEvictionWorkload<P1>(std::string("BP_") + section + "_" + n1, reads, nodes, false);
            auto r2 = runEvictionWorkload<P2>(std::string("BP_") + section + "_" + n2, reads, nodes, false);
            auto r3 = runEvictionWorkload<P3>(std::string("BP_") + section + "_" + n3, reads, nodes, false);
            printPolicyResult(n1, r1);
            printPolicyResult(n2, r2);
            printPolicyResult(n3, r3);
        }
    };

    runWorkload("P1_ZIPFIAN", wlZipf, numNodes);
    runWorkload("P2_LOOPING", wlLoop, numNodes);
    runWorkload("P3_TEMPORAL", wlTemp, numNodes);
    runWorkload("P4_SCANRES", wlScan, numNodes);

    // Single-node control (no NUMA routing)
    if (numNodes >= 2) {
        std::cout << "\n--- P1_ZIPFIAN (single-node control) ---" << std::endl;
        auto r1 = runEvictionWorkload<P1>(std::string("BP_SN_Zipf_") + n1, wlZipf, 1, false);
        auto r2 = runEvictionWorkload<P2>(std::string("BP_SN_Zipf_") + n2, wlZipf, 1, false);
        auto r3 = runEvictionWorkload<P3>(std::string("BP_SN_Zipf_") + n3, wlZipf, 1, false);
        printPolicyResult(n1, r1);
        printPolicyResult(n2, r2);
        printPolicyResult(n3, r3);
    }
}

int main() {
    FurrBall<StandardRemarc>::Bootstrap();

    int numNodes = Numatic::GetNodeCount();
    std::cout << "=== Furrballs Multi-Policy NUMA Benchmark ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << std::endl;
    std::cout << "Iterations: " << ITERATIONS << std::endl;
    std::cout << "Small config: " << SMALL_PAGES << " pages/node (" << SMALL_HOT_KEYS << " hot keys, "
              << std::fixed << std::setprecision(1) << 100.0 * SMALL_HOT_KEYS / ZIPF_UNIVERSE
              << "% of " << ZIPF_UNIVERSE << " universe)" << std::endl;
    std::cout << "Read ops/workload: " << READ_OPS << std::endl;
    std::cout << "MinDesire update interval: " << DESIRE_UPDATE_INTERVAL << std::endl;
    std::cout << "Policies: ARC, REMARC, AUG-ADAPT" << std::endl;
    std::cout << std::endl;

    runFullBenchmark<ArcPolicy, StandardRemarc, AugAdaptPolicy>("ARC", "REMARC", "AUG-ADAPT", numNodes);

    FurrBall<StandardRemarc>::Shutdown();
    return 0;
}
