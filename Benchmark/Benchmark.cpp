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
#include <sstream>
#include <functional>

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;

static constexpr int ITERATIONS = 10;
static constexpr size_t PAGE_SIZE = 4096;
static constexpr size_t VAL_SIZE = 64;
static constexpr size_t KEYS_PER_PAGE = PAGE_SIZE / VAL_SIZE;

static constexpr size_t SMALL_PAGES = 16;
static constexpr size_t SMALL_HOT_KEYS = (SMALL_PAGES - 1) * KEYS_PER_PAGE;
static constexpr size_t TINY_PAGES = 4;
static constexpr size_t TINY_HOT_KEYS = (TINY_PAGES - 1) * KEYS_PER_PAGE;

static constexpr size_t ZIPF_UNIVERSE = 10000;
static constexpr size_t LOOP_UNIVERSE = 2000;
static constexpr size_t TEMPORAL_UNIVERSE = 10000;
static constexpr size_t TEMPORAL_PERIOD = 4;
static constexpr size_t SCAN_HOT = 1000;
static constexpr size_t SCAN_UNIVERSE = 10000;

static constexpr size_t READ_OPS = 100000;
static constexpr size_t DESIRE_UPDATE_INTERVAL = 500;

// =====================================================================
//  Helpers
// =====================================================================

static double computePercentile(std::vector<ns>& v, double pct) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[static_cast<size_t>(pct * v.size() / 100.0)].count();
}

struct SampleStats {
    double mean, stddev, min, max;
};

static SampleStats computeSampleStats(const std::vector<double>& v) {
    SampleStats s{};
    if (v.empty()) return s;
    double sum = 0;
    s.min = v[0]; s.max = v[0];
    for (auto x : v) { sum += x; if (x < s.min) s.min = x; if (x > s.max) s.max = x; }
    s.mean = sum / v.size();
    double var = 0;
    for (auto x : v) { double d = x - s.mean; var += d * d; }
    s.stddev = (v.size() > 1) ? std::sqrt(var / (v.size() - 1)) : 0;
    return s;
}

static std::string fmtStats(const SampleStats& s) {
    std::ostringstream os;
    os << std::fixed << std::setprecision(0)
       << std::setw(6) << s.mean << " ns p50 (stddev " << std::setprecision(1) << s.stddev
       << ", min " << std::setprecision(0) << s.min << ", max " << s.max << ")";
    return os.str();
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
static FurrBall<Policy>* createSmallFB(const std::string& dbPath, bool threadLocal = false, size_t pages = SMALL_PAGES) {
    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    nc.UseThreadLocalRouting = threadLocal;
    FurrConfig fc;
    fc.EnableLogging = false;
    fc.EnableNUMA = true;
    fc.PageSize = PAGE_SIZE;
    fc.InitialPageCount = pages;
    fc.IsVolatile = true;
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

    std::cout << std::string(100, '-') << std::endl;
    std::cout << "A1: Pure NUMA Cross-Node Overhead (" << name << ", thread-local, 500 keys)" << std::endl;
    std::cout << std::string(100, '-') << std::endl;

    size_t numKeys = 500;
    size_t ops = 10000;
    std::vector<char> val(VAL_SIZE, 'X');
    std::vector<char> out(VAL_SIZE + 64);

    for (int it = 0; it < ITERATIONS; it++) {
        FB* fb = createLargeFB<Policy>(std::string("Bench_") + name + "_A1_" + std::to_string(it), true);
        if (!fb) continue;

        for (size_t k = 0; k < numKeys; k++) {
            Numatic::PinCurrentThreadToNode(0);
            fb->Set("a1_" + std::to_string(k), val.data(), VAL_SIZE);
        }

        std::vector<ns> localLat(ops), crossLat(ops);

        for (size_t i = 0; i < ops; i++) {
            size_t k = i % numKeys;
            size_t os = 0;
            auto t0 = Clock::now();
            fb->Get("a1_" + std::to_string(k), out.data(), out.size(), os);
            localLat[i] = Clock::now() - t0;
        }

        std::thread crossThread([&]() {
            Numatic::PinCurrentThreadToNode(1);
            for (size_t i = 0; i < ops; i++) {
                size_t k = i % numKeys;
                size_t os = 0;
                auto t0 = Clock::now();
                fb->Get("a1_" + std::to_string(k), out.data(), out.size(), os);
                crossLat[i] = Clock::now() - t0;
            }
        });
        crossThread.join();

        delete fb;

        double lp50 = computePercentile(localLat, 50);
        double cp50 = computePercentile(crossLat, 50);
        double oh50 = (lp50 > 0) ? (cp50 - lp50) / lp50 * 100.0 : 0;
        std::cout << "  [" << std::setw(2) << it << "] Local " << std::fixed << std::setprecision(0)
                  << std::setw(5) << lp50 << " ns | Cross " << std::setw(5) << cp50
                  << " ns | Cross-OH " << std::setprecision(1) << std::setw(5) << oh50 << "%" << std::endl;
    }
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
//  Policy result with latency breakdown
// =====================================================================

struct LatencyBreakdown {
    double localHitP50 = 0;
    double remoteHitP50 = 0;
    double missP50 = 0;
    size_t localHitCount = 0;
    size_t remoteHitCount = 0;
    size_t missCount = 0;
};

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
    LatencyBreakdown breakdown;
};

// =====================================================================
//  Policy workload runners
// =====================================================================

template<typename Policy>
static PolicyResult<Policy> runEvictionWorkload(
    const std::string& dbName,
    const std::vector<uint64_t>& reads,
    int numNodes,
    bool threadLocal,
    size_t pages = SMALL_PAGES)
{
    using FB = FurrBall<Policy>;
    PolicyResult<Policy> result;

    FB* fb = createSmallFB<Policy>(dbName, threadLocal, pages);
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
    std::vector<ns> hitLats, missLats;
    size_t desireCounter = 0;

    for (size_t i = 0; i < reads.size(); i++) {
        std::string key = "ev_" + std::to_string(reads[i]);
        size_t os = 0;
        auto t0 = Clock::now();
        fb->Get(key, out.data(), out.size(), os);
        getLats[i] = Clock::now() - t0;
        if (os > 0) hitLats.push_back(getLats[i]); else missLats.push_back(getLats[i]);

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

    result.breakdown.localHitP50 = hitLats.empty() ? 0 : computePercentile(hitLats, 50);
    result.breakdown.missP50 = missLats.empty() ? 0 : computePercentile(missLats, 50);
    result.breakdown.localHitCount = hitLats.size();
    result.breakdown.missCount = missLats.size();

    delete fb;
    return result;
}

template<typename Policy>
static PolicyResult<Policy> runMultiNodeWorkload(
    const std::string& dbName,
    const std::vector<uint64_t>& reads,
    int numNodes,
    size_t pages = SMALL_PAGES,
    bool enableDesireScan = true)
{
    using FB = FurrBall<Policy>;
    PolicyResult<Policy> result;

    FB* fb = createSmallFB<Policy>(dbName, true, pages);
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

    if (enableDesireScan) {
        for (int n = 0; n < numNodes; n++) fb->UpdateMinDesire(n);
    }

    unsigned int hitsBefore = fb->Stats.GetHitCount();
    unsigned int missesBefore = fb->Stats.GetMissCount();
    unsigned int evictBefore = fb->Stats.GetEvictionCount();
    unsigned int migrateBefore = fb->Stats.GetMigrationCount();
    unsigned int frozenBefore = fb->Stats.GetFrozenPageHits();
    unsigned int localHitsBefore = fb->Stats.GetLocalHitCount();

    size_t readsPerNode = reads.size() / numNodes;

    struct NodeResult {
        std::vector<ns> all;
        std::vector<ns> localHits;
        std::vector<ns> remoteHits;
        std::vector<ns> misses;
        size_t desireCounter = 0;
    };
    std::vector<NodeResult> nodeResults(numNodes);

    std::vector<std::thread> readThreads(numNodes);
    for (int n = 0; n < numNodes; n++) {
        readThreads[n] = std::thread([&, n]() {
            Numatic::PinCurrentThreadToNode(n);
            std::mt19937_64 rng(n * 42 + 7);
            auto& nr = nodeResults[n];
            nr.all.resize(readsPerNode);
            for (size_t i = 0; i < readsPerNode; i++) {
                size_t idx = rng() % reads.size();
                size_t keyVal = reads[idx];
                size_t ownerNode = keyVal / keysPerNode;
                if (ownerNode >= (size_t)numNodes) ownerNode = numNodes - 1;
                bool isLocal = (ownerNode == (size_t)n);

                std::string key = "mn_" + std::to_string(keyVal);
                size_t os = 0;
                auto t0 = Clock::now();
                fb->Get(key, out.data(), out.size(), os);
                nr.all[i] = Clock::now() - t0;

                if (os > 0) {
                    if (isLocal) nr.localHits.push_back(nr.all[i]);
                    else nr.remoteHits.push_back(nr.all[i]);
                } else {
                    nr.misses.push_back(nr.all[i]);
                }

                if (enableDesireScan) {
                    nr.desireCounter++;
                    if (nr.desireCounter >= DESIRE_UPDATE_INTERVAL) {
                        for (int nn = 0; nn < numNodes; nn++) fb->UpdateMinDesire(nn);
                        nr.desireCounter = 0;
                    }
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

    std::vector<ns> allLats, localHitLats, remoteHitLats, missLats;
    for (auto& nr : nodeResults) {
        allLats.insert(allLats.end(), nr.all.begin(), nr.all.end());
        localHitLats.insert(localHitLats.end(), nr.localHits.begin(), nr.localHits.end());
        remoteHitLats.insert(remoteHitLats.end(), nr.remoteHits.begin(), nr.remoteHits.end());
        missLats.insert(missLats.end(), nr.misses.begin(), nr.misses.end());
    }
    result.getP50 = computePercentile(allLats, 50);
    result.getP99 = computePercentile(allLats, 99);

    result.breakdown.localHitP50 = localHitLats.empty() ? 0 : computePercentile(localHitLats, 50);
    result.breakdown.remoteHitP50 = remoteHitLats.empty() ? 0 : computePercentile(remoteHitLats, 50);
    result.breakdown.missP50 = missLats.empty() ? 0 : computePercentile(missLats, 50);
    result.breakdown.localHitCount = localHitLats.size();
    result.breakdown.remoteHitCount = remoteHitLats.size();
    result.breakdown.missCount = missLats.size();

    delete fb;
    return result;
}

// =====================================================================
//  Printers
// =====================================================================

static void printPolicyResult(const char* name, const auto& r) {
    std::cout << "  " << std::left << std::setw(12) << name << " | "
              << std::right << std::fixed << std::setprecision(1) << std::setw(6) << r.hitRate << "% HR | "
              << std::setw(6) << r.localHitRate << "% LHR | "
              << std::setw(6) << r.migrations << " mig | "
              << std::setw(6) << r.evictions << " ev | "
              << std::setw(5) << r.frozenHits << " fz | "
              << std::setw(6) << std::setprecision(0) << r.getP50 << " ns p50"
              << std::endl;
    std::cout << "               |   lh " << std::setw(3) << r.breakdown.localHitCount
              << " rh " << std::setw(3) << r.breakdown.remoteHitCount
              << " mi " << std::setw(3) << r.breakdown.missCount
              << " | lh " << std::setw(5) << std::setprecision(0) << r.breakdown.localHitP50
              << " rh " << std::setw(5) << r.breakdown.remoteHitP50
              << " mi " << std::setw(5) << r.breakdown.missP50 << " ns"
              << std::endl;
}

// =====================================================================
//  Policy ablation: isolate each REMARC feature
// =====================================================================

template<typename P1, typename P2, typename P3>
void runPolicyAblation(const char* n1, const char* n2, const char* n3,
                       int numNodes, const std::vector<uint64_t>& reads) {
    if (numNodes < 2) return;

    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "POLICY ABLATION (P1 Zipfian, 10% cap, " << ITERATIONS << " iters)" << std::endl;
    std::cout << "  Step A: ARC (baseline)" << std::endl;
    std::cout << "  Step B: REMARC scoring only (no UpdateMinDesire)" << std::endl;
    std::cout << "  Step C: REMARC + migration (UpdateMinDesire enabled)" << std::endl;
    std::cout << "  Step D: AUG-ADAPT + migration (self-tuning)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;

    auto runStep = [&](const char* stepName, auto policyTag, const char* pName,
                       bool desireScan) -> std::vector<double> {
        using P = decltype(policyTag);
        std::vector<double> p50s;
        for (int it = 0; it < ITERATIONS; it++) {
            auto r = runMultiNodeWorkload<P>(
                std::string("ABL_") + stepName + "_" + std::to_string(it) + "_" + pName,
                reads, numNodes, SMALL_PAGES, desireScan);
            p50s.push_back(r.getP50);
            std::cout << "  [" << std::setw(2) << it << "] " << std::left << std::setw(6) << stepName
                      << " " << std::setw(10) << pName << " | "
                      << std::right << std::fixed << std::setprecision(1) << std::setw(6) << r.hitRate << "% HR | "
                      << std::setw(4) << r.migrations << " mig | "
                      << std::setw(6) << std::setprecision(0) << r.getP50 << " ns p50"
                      << std::endl;
        }
        auto s = computeSampleStats(p50s);
        std::cout << "  AVG  " << std::left << std::setw(6) << stepName
                  << " " << std::setw(10) << pName << " | " << fmtStats(s) << std::endl;
        return p50s;
    };

    auto a = runStep("A", P1{}, n1, false);
    auto b = runStep("B", P2{}, n2, false);
    auto c = runStep("C", P2{}, n2, true);
    auto d = runStep("D", P3{}, n3, true);

    auto sa = computeSampleStats(a);
    auto sb = computeSampleStats(b);
    auto sc = computeSampleStats(c);
    auto sd = computeSampleStats(d);

    std::cout << "\n  Delta B-A (TempCtrl scoring): " << std::fixed << std::setprecision(0)
              << (sb.mean - sa.mean) << " ns" << std::endl;
    std::cout << "  Delta C-B (Migration):         " << (sc.mean - sb.mean) << " ns" << std::endl;
    std::cout << "  Delta D-C (Self-tuning):        " << (sd.mean - sc.mean) << " ns" << std::endl;
}

// =====================================================================
//  Main benchmark orchestration
// =====================================================================

template<typename P1, typename P2, typename P3>
void runFullBenchmark(const char* n1, const char* n2, const char* n3, int numNodes) {
    std::mt19937_64 rng(42);

    // === ARCHITECTURE ===
    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "ARCHITECTURE VALIDATION (" << ITERATIONS << " iterations)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;

    runA1_PureNUMA<P1>(n1, numNodes);
    runA2_Routing<P1>(n1, numNodes);

    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "A3: Thread Scaling (all policies)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;
    runA3_Scaling<P1>(n1, numNodes);
    runA3_Scaling<P2>(n2, numNodes);
    runA3_Scaling<P3>(n3, numNodes);

    // === POLICY COMPARISON ===
    auto wlZipf = genZipfian(READ_OPS, ZIPF_UNIVERSE, 0.99, rng);
    rng.seed(42);
    auto wlScan = genScanResistant(READ_OPS, SCAN_HOT, SCAN_UNIVERSE, rng);
    auto wlLoop = genLooping(READ_OPS, LOOP_UNIVERSE);
    auto wlTemp = genTemporalShift(READ_OPS, TEMPORAL_UNIVERSE, TEMPORAL_PERIOD);

    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << "POLICY COMPARISON: Eviction Pressure (VOLATILE mode, " << ITERATIONS << " iters)" << std::endl;
    std::cout << std::string(100, '=') << std::endl;

    auto runWorkload = [&](const char* section, const std::vector<uint64_t>& reads, int nodes, size_t pages, const char* tag) {
        size_t hotKeys = (pages - 1) * KEYS_PER_PAGE;
        size_t universe = 0;
        for (auto k : reads) if (k > universe) universe = k;
        universe++;
        std::cout << "\n--- " << section << " (" << tag << ": " << pages << " pg, "
                  << hotKeys << " hot, " << universe << " univ, "
                  << std::fixed << std::setprecision(0) << 100.0 * hotKeys / universe << "% cap) ---" << std::endl;
        if (nodes >= 2) {
            auto r1 = runMultiNodeWorkload<P1>(std::string("BP_") + section + "_" + tag + "_" + n1, reads, nodes, pages);
            auto r2 = runMultiNodeWorkload<P2>(std::string("BP_") + section + "_" + tag + "_" + n2, reads, nodes, pages);
            auto r3 = runMultiNodeWorkload<P3>(std::string("BP_") + section + "_" + tag + "_" + n3, reads, nodes, pages);
            printPolicyResult(n1, r1);
            printPolicyResult(n2, r2);
            printPolicyResult(n3, r3);
        } else {
            auto r1 = runEvictionWorkload<P1>(std::string("BP_") + section + "_" + tag + "_" + n1, reads, nodes, false, pages);
            auto r2 = runEvictionWorkload<P2>(std::string("BP_") + section + "_" + tag + "_" + n2, reads, nodes, false, pages);
            auto r3 = runEvictionWorkload<P3>(std::string("BP_") + section + "_" + tag + "_" + n3, reads, nodes, false, pages);
            printPolicyResult(n1, r1);
            printPolicyResult(n2, r2);
            printPolicyResult(n3, r3);
        }
    };

    for (size_t pages : {SMALL_PAGES, TINY_PAGES}) {
        const char* tag = (pages == SMALL_PAGES) ? "10pct" : "3pct";
        runWorkload("P1_ZIPFIAN", wlZipf, numNodes, pages, tag);
        runWorkload("P2_LOOPING", wlLoop, numNodes, pages, tag);
        runWorkload("P3_TEMPORAL", wlTemp, numNodes, pages, tag);
        runWorkload("P4_SCANRES", wlScan, numNodes, pages, tag);
    }

    // Single-node control
    if (numNodes >= 2) {
        std::cout << "\n" << std::string(100, '-') << std::endl;
        std::cout << "SINGLE-NODE CONTROL (no thread-local routing)" << std::endl;
        std::cout << std::string(100, '-') << std::endl;
        for (size_t pages : {SMALL_PAGES, TINY_PAGES}) {
            const char* tag = (pages == SMALL_PAGES) ? "SN10" : "SN3";
            auto runSN = [&](const char* section, const std::vector<uint64_t>& reads) {
                size_t hotKeys = (pages - 1) * KEYS_PER_PAGE;
                std::cout << "\n--- " << section << " (" << tag << ": " << hotKeys << " hot) ---" << std::endl;
                auto r1 = runEvictionWorkload<P1>(std::string("BP_") + section + "_" + tag + "_" + n1, reads, 1, false, pages);
                auto r2 = runEvictionWorkload<P2>(std::string("BP_") + section + "_" + tag + "_" + n2, reads, 1, false, pages);
                auto r3 = runEvictionWorkload<P3>(std::string("BP_") + section + "_" + tag + "_" + n3, reads, 1, false, pages);
                printPolicyResult(n1, r1);
                printPolicyResult(n2, r2);
                printPolicyResult(n3, r3);
            };
            runSN("ZIPF", wlZipf);
            runSN("LOOP", wlLoop);
            runSN("TEMP", wlTemp);
            runSN("SCAN", wlScan);
        }
    }

    // === POLICY ABLATION ===
    runPolicyAblation<P1, P2, P3>(n1, n2, n3, numNodes, wlZipf);

    // === LATENCY GAP INVESTIGATION ===
    if (numNodes >= 2) {
        static constexpr int INV_ITERS = 10;

        std::cout << "\n" << std::string(100, '=') << std::endl;
        std::cout << "LATENCY GAP INVESTIGATION (P1 Zipfian, 10% cap, "
                  << INV_ITERS << " iters each)" << std::endl;
        std::cout << "Hypothesis: ARC p50 is 2-5x slower than REMARC in multi-node" << std::endl;
        std::cout << "  Run A: NoScan  — disable UpdateMinDesire for all policies" << std::endl;
        std::cout << "  Run B: RevOrd  — reverse policy order (AUG first, ARC last)" << std::endl;
        std::cout << "  Run C: EqScan  — ARC gets page-warming via UpdateMinDesire" << std::endl;
        std::cout << std::string(100, '=') << std::endl;

        auto printInvIter = [&](int it, const char* name, const auto& r) {
            std::cout << "  [" << std::setw(2) << it << "] "
                      << std::left << std::setw(12) << name << " | "
                      << std::right << std::fixed << std::setprecision(1) << std::setw(6) << r.hitRate << "% HR | "
                      << std::setw(6) << r.migrations << " mig | "
                      << std::setw(6) << std::setprecision(0) << r.getP50 << " ns p50"
                      << std::endl;
        };

        auto invAvg = [](const std::vector<double>& v) -> double {
            if (v.empty()) return 0;
            double s = 0; for (auto x : v) s += x; return s / v.size();
        };

        // --- Run A: NoScan ---
        {
            std::cout << "\n--- Run A: NoScan (UpdateMinDesire DISABLED for all) ---" << std::endl;
            std::vector<double> a_p50, r_p50, u_p50;
            for (int it = 0; it < INV_ITERS; it++) {
                auto r1 = runMultiNodeWorkload<P1>("INV_A_" + std::to_string(it) + "_" + n1, wlZipf, numNodes, SMALL_PAGES, false);
                auto r2 = runMultiNodeWorkload<P2>("INV_A_" + std::to_string(it) + "_" + n2, wlZipf, numNodes, SMALL_PAGES, false);
                auto r3 = runMultiNodeWorkload<P3>("INV_A_" + std::to_string(it) + "_" + n3, wlZipf, numNodes, SMALL_PAGES, false);
                printInvIter(it, n1, r1);
                printInvIter(it, n2, r2);
                printInvIter(it, n3, r3);
                a_p50.push_back(r1.getP50);
                r_p50.push_back(r2.getP50);
                u_p50.push_back(r3.getP50);
            }
            std::cout << "  AVG  " << std::left << std::setw(14) << n1 << " " << std::setw(14) << n2 << " " << std::setw(14) << n3 << std::endl;
            std::cout << "  p50  " << std::fixed << std::setprecision(0)
                      << std::setw(10) << invAvg(a_p50) << "    "
                      << std::setw(10) << invAvg(r_p50) << "    "
                      << std::setw(10) << invAvg(u_p50) << " ns" << std::endl;
        }

        // --- Run B: ReverseOrder ---
        {
            std::cout << "\n--- Run B: ReverseOrder (AUG-ADAPT first, ARC last) ---" << std::endl;
            std::vector<double> a_p50, r_p50, u_p50;
            for (int it = 0; it < INV_ITERS; it++) {
                auto r3 = runMultiNodeWorkload<P3>("INV_B_" + std::to_string(it) + "_" + n3, wlZipf, numNodes, SMALL_PAGES);
                auto r2 = runMultiNodeWorkload<P2>("INV_B_" + std::to_string(it) + "_" + n2, wlZipf, numNodes, SMALL_PAGES);
                auto r1 = runMultiNodeWorkload<P1>("INV_B_" + std::to_string(it) + "_" + n1, wlZipf, numNodes, SMALL_PAGES);
                printInvIter(it, n3, r3);
                printInvIter(it, n2, r2);
                printInvIter(it, n1, r1);
                a_p50.push_back(r1.getP50);
                r_p50.push_back(r2.getP50);
                u_p50.push_back(r3.getP50);
            }
            std::cout << "  AVG  " << std::left << std::setw(14) << n1 << " " << std::setw(14) << n2 << " " << std::setw(14) << n3 << std::endl;
            std::cout << "  p50  " << std::fixed << std::setprecision(0)
                      << std::setw(10) << invAvg(a_p50) << "    "
                      << std::setw(10) << invAvg(r_p50) << "    "
                      << std::setw(10) << invAvg(u_p50) << " ns" << std::endl;
        }

        // --- Run C: EqualScan ---
        {
            std::cout << "\n--- Run C: EqualScan (ARC page-warming enabled in UpdateMinDesire) ---" << std::endl;
            std::vector<double> a_p50, r_p50, u_p50;
            for (int it = 0; it < INV_ITERS; it++) {
                auto r1 = runMultiNodeWorkload<P1>("INV_C_" + std::to_string(it) + "_" + n1, wlZipf, numNodes, SMALL_PAGES);
                auto r2 = runMultiNodeWorkload<P2>("INV_C_" + std::to_string(it) + "_" + n2, wlZipf, numNodes, SMALL_PAGES);
                auto r3 = runMultiNodeWorkload<P3>("INV_C_" + std::to_string(it) + "_" + n3, wlZipf, numNodes, SMALL_PAGES);
                printInvIter(it, n1, r1);
                printInvIter(it, n2, r2);
                printInvIter(it, n3, r3);
                a_p50.push_back(r1.getP50);
                r_p50.push_back(r2.getP50);
                u_p50.push_back(r3.getP50);
            }
            std::cout << "  AVG  " << std::left << std::setw(14) << n1 << " " << std::setw(14) << n2 << " " << std::setw(14) << n3 << std::endl;
            std::cout << "  p50  " << std::fixed << std::setprecision(0)
                      << std::setw(10) << invAvg(a_p50) << "    "
                      << std::setw(10) << invAvg(r_p50) << "    "
                      << std::setw(10) << invAvg(u_p50) << " ns" << std::endl;
        }

        std::cout << "\n" << std::string(100, '-') << std::endl;
        std::cout << "INTERPRETATION:" << std::endl;
        std::cout << "  If gap disappears in A: latency gap is from UpdateMinDesire scan warming" << std::endl;
        std::cout << "  If gap disappears in B: latency gap is from run order (cold-start penalty)" << std::endl;
        std::cout << "  If gap disappears in C: page-header touch equalizes warming (confirms A)" << std::endl;
        std::cout << "  If gap persists in A+B+C: real policy-driven latency difference" << std::endl;
        std::cout << std::string(100, '-') << std::endl;
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
