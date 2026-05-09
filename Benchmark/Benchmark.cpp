#include "Furrballs.h"
#include "Numatic.h"
#include "BaselineCache.h"
#include "SharedNothingCache.h"
#include "NUMAAllocCache.h"
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <iomanip>
#include <cmath>
#include <random>

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;

struct BenchResult {
    std::string name;
    size_t ops = 0;
    double durationMs = 0;
    double opsPerSec = 0;
    double avgLatencyNs = 0;
    double p50Ns = 0;
    double p95Ns = 0;
    double p99Ns = 0;
    double stddevNs = 0;
};

static void printResult(const BenchResult& r) {
    std::cout << std::left << std::setw(30) << r.name << " | "
              << std::right << std::setw(8) << r.ops << " ops | "
              << std::setw(10) << std::fixed << std::setprecision(1) << r.opsPerSec << " ops/s | "
              << "avg " << std::setw(8) << std::fixed << std::setprecision(0) << r.avgLatencyNs << " ns | "
              << "p50 " << std::setw(8) << std::fixed << std::setprecision(0) << r.p50Ns << " ns | "
              << "p95 " << std::setw(8) << std::fixed << std::setprecision(0) << r.p95Ns << " ns | "
              << "p99 " << std::setw(8) << std::fixed << std::setprecision(0) << r.p99Ns << " ns | "
              << "stddev " << std::setw(6) << std::fixed << std::setprecision(0) << r.stddevNs << " ns"
              << std::endl;
}

static double computeStddev(const std::vector<ns>& sorted, double mean) {
    double sum = 0;
    for (auto& l : sorted) {
        double diff = l.count() - mean;
        sum += diff * diff;
    }
    return std::sqrt(sum / sorted.size());
}

template<typename Policy>
struct ThreadArg {
    FurrBall<Policy>* fb;
    size_t startKey;
    size_t endKey;
    size_t valueSize;
    int pinNode;
    bool doSet;
    std::vector<ns>* latencies;
};

template<typename Policy>
static void threadWorker(ThreadArg<Policy>* arg) {
    if (arg->pinNode >= 0) {
        Numatic::PinCurrentThreadToNode(arg->pinNode);
    }

    std::vector<char> value(arg->valueSize, 'X');
    std::vector<char> outBuf(arg->valueSize + 64);

    size_t numOps = arg->endKey - arg->startKey;
    arg->latencies->resize(numOps);

    for (size_t i = 0; i < numOps; i++) {
        std::string key = "mt_key_" + std::to_string(arg->startKey + i);

        if (arg->doSet) {
            auto t0 = Clock::now();
            arg->fb->Set(key, value.data(), arg->valueSize);
            arg->latencies->at(i) = Clock::now() - t0;
        } else {
            size_t outSize = 0;
            auto t0 = Clock::now();
            arg->fb->Get(key, outBuf.data(), outBuf.size(), outSize);
            arg->latencies->at(i) = Clock::now() - t0;
        }
    }
}

template<typename Policy>
static void warmupSet(FurrBall<Policy>* fb, const std::string& prefix, size_t numOps, size_t valueSize) {
    std::vector<char> value(valueSize, 'X');
    for (size_t i = 0; i < numOps; i++) {
        std::string key = prefix + "_key_" + std::to_string(i);
        fb->Set(key, value.data(), valueSize);
    }
}

template<typename Policy>
static void warmupSetMT(FurrBall<Policy>* fb, int numThreads, size_t keysPerThread, size_t valueSize, bool crossNode, int numNodes) {
    std::vector<ThreadArg<Policy>> args(numThreads);
    std::vector<std::thread> threads(numThreads);
    std::vector<std::vector<ns>> dummy(numThreads);

    for (int t = 0; t < numThreads; t++) {
        args[t].fb = fb;
        args[t].startKey = t * keysPerThread;
        args[t].endKey = args[t].startKey + keysPerThread;
        args[t].valueSize = valueSize;
        args[t].doSet = true;
        args[t].latencies = &dummy[t];
        args[t].pinNode = crossNode ? ((t + 1) % numNodes) : (t % numNodes);
    }

    for (int t = 0; t < numThreads; t++)
        threads[t] = std::thread(threadWorker<Policy>, &args[t]);
    for (auto& th : threads) th.join();
}

template<typename Policy>
static BenchResult benchSingleThread(FurrBall<Policy>* fb, const std::string& prefix, size_t numOps, size_t valueSize, bool doSet) {
    BenchResult result;
    result.name = prefix + (doSet ? " Set" : " Get");
    result.ops = numOps;

    std::vector<char> value(valueSize, 'X');
    std::vector<char> outBuf(valueSize + 64);
    std::vector<ns> latencies(numOps);

    if (!doSet) {
        warmupSet(fb, prefix, numOps, valueSize);
    }

    auto start = Clock::now();

    for (size_t i = 0; i < numOps; i++) {
        std::string key = prefix + "_key_" + std::to_string(i);

        if (doSet) {
            auto t0 = Clock::now();
            fb->Set(key, value.data(), valueSize);
            latencies[i] = Clock::now() - t0;
        } else {
            size_t outSize = 0;
            auto t0 = Clock::now();
            fb->Get(key, outBuf.data(), outBuf.size(), outSize);
            latencies[i] = Clock::now() - t0;
        }
    }

    auto end = Clock::now();
    result.durationMs = std::chrono::duration<double, std::milli>(end - start).count();
    result.opsPerSec = (result.durationMs > 0) ? (numOps / (result.durationMs / 1000.0)) : 0;

    std::sort(latencies.begin(), latencies.end());
    double totalNs = 0;
    for (auto& l : latencies) totalNs += l.count();
    result.avgLatencyNs = totalNs / numOps;
    result.stddevNs = computeStddev(latencies, result.avgLatencyNs);
    result.p50Ns = latencies[numOps / 2].count();
    result.p95Ns = latencies[numOps * 95 / 100].count();
    result.p99Ns = latencies[numOps * 99 / 100].count();

    return result;
}

template<typename Policy>
static BenchResult benchMultiThread(FurrBall<Policy>* fb, const std::string& name, int numThreads, int numNodes, size_t keysPerThread, size_t valueSize, bool doSet, bool crossNode) {
    BenchResult result;
    result.name = name;

    std::vector<ThreadArg<Policy>> args(numThreads);
    std::vector<std::thread> threads(numThreads);
    std::vector<std::vector<ns>> allLatencies(numThreads);

    for (int t = 0; t < numThreads; t++) {
        args[t].fb = fb;
        args[t].startKey = t * keysPerThread;
        args[t].endKey = args[t].startKey + keysPerThread;
        args[t].valueSize = valueSize;
        args[t].doSet = doSet;
        args[t].latencies = &allLatencies[t];
        args[t].pinNode = crossNode ? ((t + 1) % numNodes) : (t % numNodes);
    }

    if (!doSet) {
        warmupSetMT(fb, numThreads, keysPerThread, valueSize, crossNode, numNodes);
    }

    auto start = Clock::now();

    for (int t = 0; t < numThreads; t++)
        threads[t] = std::thread(threadWorker<Policy>, &args[t]);
    for (auto& th : threads) th.join();

    auto end = Clock::now();
    size_t totalOps = numThreads * keysPerThread;
    result.ops = totalOps;
    result.durationMs = std::chrono::duration<double, std::milli>(end - start).count();
    result.opsPerSec = (result.durationMs > 0) ? (totalOps / (result.durationMs / 1000.0)) : 0;

    std::vector<ns> flat;
    for (auto& v : allLatencies)
        flat.insert(flat.end(), v.begin(), v.end());

    if (!flat.empty()) {
        std::sort(flat.begin(), flat.end());
        double totalNs = 0;
        for (auto& l : flat) totalNs += l.count();
        result.avgLatencyNs = totalNs / flat.size();
        result.stddevNs = computeStddev(flat, result.avgLatencyNs);
        result.p50Ns = flat[flat.size() / 2].count();
        result.p95Ns = flat[flat.size() * 95 / 100].count();
        result.p99Ns = flat[flat.size() * 99 / 100].count();
    }

    return result;
}

struct IterationResult {
    double avgOpsPerSec = 0;
    double avgP50 = 0;
    double avgP99 = 0;
    double avgStddev = 0;
};

template<typename Policy>
static IterationResult runIterations(FurrBall<Policy>* fb, const std::function<BenchResult(FurrBall<Policy>*)>& benchFn, int iterations) {
    IterationResult ir;
    std::vector<double> opsPerSec(iterations);
    std::vector<double> p50s(iterations);
    std::vector<double> p99s(iterations);
    std::vector<double> stddevs(iterations);

    for (int i = 0; i < iterations; i++) {
        BenchResult r = benchFn(fb);
        opsPerSec[i] = r.opsPerSec;
        p50s[i] = r.p50Ns;
        p99s[i] = r.p99Ns;
        stddevs[i] = r.stddevNs;
    }

    for (auto& v : opsPerSec) ir.avgOpsPerSec += v;
    ir.avgOpsPerSec /= iterations;
    for (auto& v : p50s) ir.avgP50 += v;
    ir.avgP50 /= iterations;
    for (auto& v : p99s) ir.avgP99 += v;
    ir.avgP99 /= iterations;
    for (auto& v : stddevs) ir.avgStddev += v;
    ir.avgStddev /= iterations;

    return ir;
}

struct RoutingBenchResult {
    std::string name;
    double setP50 = 0, setP99 = 0;
    double selfGetP50 = 0, selfGetP99 = 0;
    double crossGetP50 = 0, crossGetP99 = 0;
};

template<typename Policy>
static RoutingBenchResult benchRoutingStrategy(FurrBall<Policy>* fb, const std::string& name, int numNodes, size_t keysPerThread, size_t valueSize) {
    RoutingBenchResult result;
    result.name = name;

    struct ThreadResult {
        std::vector<ns> setLatencies;
        std::vector<ns> selfGetLatencies;
        std::vector<ns> crossGetLatencies;
    };

    std::vector<ThreadResult> threadResults(numNodes);
    std::vector<std::thread> threads(numNodes);

    auto worker = [&](int nodeId, ThreadResult& tr) {
        Numatic::PinCurrentThreadToNode(nodeId);

        std::vector<char> value(valueSize, 'X');
        std::vector<char> outBuf(valueSize + 64);

        tr.setLatencies.resize(keysPerThread);
        tr.selfGetLatencies.resize(keysPerThread);
        tr.crossGetLatencies.resize(keysPerThread);

        std::string prefix = "route_t" + std::to_string(nodeId) + "_";

        for (size_t i = 0; i < keysPerThread; i++) {
            std::string key = prefix + std::to_string(i);
            auto t0 = Clock::now();
            fb->Set(key, value.data(), valueSize);
            tr.setLatencies[i] = Clock::now() - t0;
        }

        int otherNode = (nodeId + 1) % numNodes;
        std::string otherPrefix = "route_t" + std::to_string(otherNode) + "_";

        for (size_t i = 0; i < keysPerThread; i++) {
            std::string key = prefix + std::to_string(i);
            size_t outSize = 0;
            auto t0 = Clock::now();
            fb->Get(key, outBuf.data(), outBuf.size(), outSize);
            tr.selfGetLatencies[i] = Clock::now() - t0;
        }

        for (size_t i = 0; i < keysPerThread; i++) {
            std::string key = otherPrefix + std::to_string(i);
            size_t outSize = 0;
            auto t0 = Clock::now();
            fb->Get(key, outBuf.data(), outBuf.size(), outSize);
            tr.crossGetLatencies[i] = Clock::now() - t0;
        }
    };

    for (int t = 0; t < numNodes; t++)
        threads[t] = std::thread(worker, t, std::ref(threadResults[t]));
    for (auto& th : threads) th.join();

    auto aggregate = [](std::vector<ns>& flat) -> std::pair<double, double> {
        if (flat.empty()) return {0, 0};
        std::sort(flat.begin(), flat.end());
        double p50 = flat[flat.size() / 2].count();
        double p99 = flat[flat.size() * 99 / 100].count();
        return {p50, p99};
    };

    std::vector<ns> allSet, allSelfGet, allCrossGet;
    for (auto& tr : threadResults) {
        allSet.insert(allSet.end(), tr.setLatencies.begin(), tr.setLatencies.end());
        allSelfGet.insert(allSelfGet.end(), tr.selfGetLatencies.begin(), tr.selfGetLatencies.end());
        allCrossGet.insert(allCrossGet.end(), tr.crossGetLatencies.begin(), tr.crossGetLatencies.end());
    }

    auto [sp50, sp99] = aggregate(allSet);
    auto [sgp50, sgp99] = aggregate(allSelfGet);
    auto [cgp50, cgp99] = aggregate(allCrossGet);

    result.setP50 = sp50; result.setP99 = sp99;
    result.selfGetP50 = sgp50; result.selfGetP99 = sgp99;
    result.crossGetP50 = cgp50; result.crossGetP99 = cgp99;

    return result;
}

template<typename Policy>
static void printStats(FurrBall<Policy>* fb) {
    std::cout << "  HitCount: " << fb->Stats.GetHitCount() << std::endl;
    std::cout << "  MissCount: " << fb->Stats.GetMissCount() << std::endl;
    std::cout << "  BytesWritten: " << fb->Stats.GetBytesWritten() << std::endl;
    std::cout << "  BytesRead: " << fb->Stats.GetBytesRead() << std::endl;
    double hr = (fb->Stats.GetHitCount() + fb->Stats.GetMissCount() > 0)
        ? 100.0 * fb->Stats.GetHitCount() / (fb->Stats.GetHitCount() + fb->Stats.GetMissCount()) : 0;
    std::cout << "  HitRate: " << std::fixed << std::setprecision(2) << hr << "%" << std::endl;
    double localRate = (fb->Stats.GetHitCount() > 0)
        ? 100.0 * fb->Stats.GetLocalHitCount() / fb->Stats.GetHitCount() : 0;
    std::cout << "  LocalHitRate: " << std::fixed << std::setprecision(2) << localRate << "%" << std::endl;
}

template<typename Policy>
static FurrBall<Policy>* createFB(const std::string& dbPath, bool threadLocal = false) {
    NumaConfig numaConfig;
    numaConfig.AllocateUsingNodePageSize = false;
    numaConfig.UseThreadLocalRouting = threadLocal;

    FurrConfig config;
    config.EnableLogging = false;
    config.EnableNUMA = true;
    config.PageSize = 4096;
    config.InitialPageCount = 2048;
    config.numaConfig = &numaConfig;

    auto* fb = FurrBall<Policy>::CreateBall(dbPath, config);
    if (!fb) {
        config.EnableNUMA = false;
        config.numaConfig = nullptr;
        fb = FurrBall<Policy>::CreateBall(dbPath, config);
    }
    return fb;
}

template<typename Policy>
static void runPolicyBenchmarks(const char* policyName, int numNodes, int iterations) {
    using FB = FurrBall<Policy>;

    std::cout << std::string(120, '#') << std::endl;
    std::cout << "POLICY: " << policyName << std::endl;
    std::cout << std::string(120, '#') << std::endl;

    std::string dbPrefix = std::string("BenchDB_") + policyName + "_";

    FB* fb = createFB<Policy>(dbPrefix + "Main");
    if (!fb) {
        std::cerr << "Cannot create FurrBall for " << policyName << std::endl;
        return;
    }

    int actualNodes = (fb && numNodes >= 2) ? numNodes : 1;

    // --- SINGLE-THREADED ---
    {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "SINGLE-THREADED (" << iterations << " iterations, averaged)" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "64-byte values, 10000 ops" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            auto setI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "64B", 10000, 64, true); }, iterations);
            auto getI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "64B", 10000, 64, false); }, iterations);
            std::cout << "Set  | " << std::fixed << std::setprecision(0)
                      << "avg " << setI.avgOpsPerSec << " ops/s | p50 " << setI.avgP50 << " ns | p99 " << setI.avgP99 << " ns | stddev " << setI.avgStddev << " ns" << std::endl;
            std::cout << "Get  | " << std::fixed << std::setprecision(0)
                      << "avg " << getI.avgOpsPerSec << " ops/s | p50 " << getI.avgP50 << " ns | p99 " << getI.avgP99 << " ns | stddev " << getI.avgStddev << " ns" << std::endl;
        }
        std::cout << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "512-byte values, 5000 ops" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            auto setI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "512B", 5000, 512, true); }, iterations);
            auto getI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "512B", 5000, 512, false); }, iterations);
            std::cout << "Set  | " << std::fixed << std::setprecision(0)
                      << "avg " << setI.avgOpsPerSec << " ops/s | p50 " << setI.avgP50 << " ns | p99 " << setI.avgP99 << " ns | stddev " << setI.avgStddev << " ns" << std::endl;
            std::cout << "Get  | " << std::fixed << std::setprecision(0)
                      << "avg " << getI.avgOpsPerSec << " ops/s | p50 " << getI.avgP50 << " ns | p99 " << getI.avgP99 << " ns | stddev " << getI.avgStddev << " ns" << std::endl;
        }
        std::cout << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "4KB values, 1000 ops" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            auto setI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "4KB", 1000, 4096, true); }, iterations);
            auto getI = runIterations<Policy>(fb, [&](FB* b){ return benchSingleThread<Policy>(b, "4KB", 1000, 4096, false); }, iterations);
            std::cout << "Set  | " << std::fixed << std::setprecision(0)
                      << "avg " << setI.avgOpsPerSec << " ops/s | p50 " << setI.avgP50 << " ns | p99 " << setI.avgP99 << " ns | stddev " << setI.avgStddev << " ns" << std::endl;
            std::cout << "Get  | " << std::fixed << std::setprecision(0)
                      << "avg " << getI.avgOpsPerSec << " ops/s | p50 " << getI.avgP50 << " ns | p99 " << getI.avgP99 << " ns | stddev " << getI.avgStddev << " ns" << std::endl;
        }
        std::cout << std::endl;
    }

    // --- MULTI-THREADED ---
    if (actualNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "MULTI-THREADED NUMA EFFECT (" << iterations << " iterations, averaged)" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "64-byte values, 2 threads, 5000 keys/thread" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            double localP50Sum = 0, crossP50Sum = 0, localP99Sum = 0, crossP99Sum = 0;
            double localOpsSum = 0, crossOpsSum = 0;

            for (int i = 0; i < iterations; i++) {
                auto getL = benchMultiThread<Policy>(fb, "Get local", 2, actualNodes, 5000, 64, false, false);
                auto getX = benchMultiThread<Policy>(fb, "Get cross", 2, actualNodes, 5000, 64, false, true);
                localP50Sum += getL.p50Ns;
                crossP50Sum += getX.p50Ns;
                localP99Sum += getL.p99Ns;
                crossP99Sum += getX.p99Ns;
                localOpsSum += getL.opsPerSec;
                crossOpsSum += getX.opsPerSec;
            }

            double localP50 = localP50Sum / iterations;
            double crossP50 = crossP50Sum / iterations;
            double localP99 = localP99Sum / iterations;
            double crossP99 = crossP99Sum / iterations;
            double overhead = (localP50 > 0) ? ((crossP50 - localP50) / localP50 * 100.0) : 0;

            std::cout << "Get local  | " << std::fixed << std::setprecision(0)
                      << "avg " << localOpsSum / iterations << " ops/s | p50 " << localP50 << " ns | p99 " << localP99 << " ns" << std::endl;
            std::cout << "Get cross | " << std::fixed << std::setprecision(0)
                      << "avg " << crossOpsSum / iterations << " ops/s | p50 " << crossP50 << " ns | p99 " << crossP99 << " ns" << std::endl;
            std::cout << "Cross-node overhead (p50): " << std::fixed << std::setprecision(1) << overhead << "%" << std::endl;
            std::cout << "Cross-node overhead (p99): " << std::fixed << std::setprecision(1)
                      << ((localP99 > 0) ? ((crossP99 - localP99) / localP99 * 100.0) : 0) << "%" << std::endl;
        }
        std::cout << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "512-byte values, 2 threads, 2000 keys/thread" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            double localP50Sum = 0, crossP50Sum = 0, localP99Sum = 0, crossP99Sum = 0;
            double localOpsSum = 0, crossOpsSum = 0;

            for (int i = 0; i < iterations; i++) {
                auto getL = benchMultiThread<Policy>(fb, "Get local", 2, actualNodes, 2000, 512, false, false);
                auto getX = benchMultiThread<Policy>(fb, "Get cross", 2, actualNodes, 2000, 512, false, true);
                localP50Sum += getL.p50Ns;
                crossP50Sum += getX.p50Ns;
                localP99Sum += getL.p99Ns;
                crossP99Sum += getX.p99Ns;
                localOpsSum += getL.opsPerSec;
                crossOpsSum += getX.opsPerSec;
            }

            double localP50 = localP50Sum / iterations;
            double crossP50 = crossP50Sum / iterations;
            double localP99 = localP99Sum / iterations;
            double crossP99 = crossP99Sum / iterations;
            double overhead = (localP50 > 0) ? ((crossP50 - localP50) / localP50 * 100.0) : 0;

            std::cout << "Get local  | " << std::fixed << std::setprecision(0)
                      << "avg " << localOpsSum / iterations << " ops/s | p50 " << localP50 << " ns | p99 " << localP99 << " ns" << std::endl;
            std::cout << "Get cross | " << std::fixed << std::setprecision(0)
                      << "avg " << crossOpsSum / iterations << " ops/s | p50 " << crossP50 << " ns | p99 " << crossP99 << " ns" << std::endl;
            std::cout << "Cross-node overhead (p50): " << std::fixed << std::setprecision(1) << overhead << "%" << std::endl;
            std::cout << "Cross-node overhead (p99): " << std::fixed << std::setprecision(1)
                      << ((localP99 > 0) ? ((crossP99 - localP99) / localP99 * 100.0) : 0) << "%" << std::endl;
        }
        std::cout << std::endl;

        // 4 threads concurrent
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "CONCURRENT THROUGHPUT (4 threads, 2500 keys/thread, mixed)" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "64-byte values, all local" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            double setOpsSum = 0, getOpsSum = 0;
            for (int i = 0; i < iterations; i++) {
                auto setR = benchMultiThread<Policy>(fb, "Set", 4, actualNodes, 2500, 64, true, false);
                auto getR = benchMultiThread<Policy>(fb, "Get", 4, actualNodes, 2500, 64, false, false);
                setOpsSum += setR.opsPerSec;
                getOpsSum += getR.opsPerSec;
            }
            std::cout << "Set (4t local) | " << std::fixed << std::setprecision(0)
                      << "avg " << setOpsSum / iterations << " ops/s" << std::endl;
            std::cout << "Get (4t local) | " << std::fixed << std::setprecision(0)
                      << "avg " << getOpsSum / iterations << " ops/s" << std::endl;
        }
        std::cout << std::endl;

        std::cout << std::string(110, '-') << std::endl;
        std::cout << "64-byte values, all cross-node" << std::endl;
        std::cout << std::string(110, '-') << std::endl;
        {
            double setOpsSum = 0, getOpsSum = 0;
            for (int i = 0; i < iterations; i++) {
                auto setR = benchMultiThread<Policy>(fb, "Set", 4, actualNodes, 2500, 64, true, true);
                auto getR = benchMultiThread<Policy>(fb, "Get", 4, actualNodes, 2500, 64, false, true);
                setOpsSum += setR.opsPerSec;
                getOpsSum += getR.opsPerSec;
            }
            std::cout << "Set (4t cross) | " << std::fixed << std::setprecision(0)
                      << "avg " << setOpsSum / iterations << " ops/s" << std::endl;
            std::cout << "Get (4t cross) | " << std::fixed << std::setprecision(0)
                      << "avg " << getOpsSum / iterations << " ops/s" << std::endl;
        }
    } else {
        std::cout << "Skipping multi-threaded NUMA benchmarks (1 node)." << std::endl;
    }

    // --- THREAD SCALING ---
    if (actualNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "THREAD SCALING (" << iterations << " iterations, averaged)" << std::endl;
        std::cout << "64-byte values, thread-local routing, local Gets, per-thread-count fresh FurrBall" << std::endl;
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "Threads | Set ops/s | Get ops/s" << std::endl;
        std::cout << "--------|----------|----------" << std::endl;
        for (int tc : {4, 8, 16, 32, 64}) {
            int kpt = (tc <= 16) ? 2500 : 1000;
            double sSum = 0, gSum = 0;
            int validIter = 0;
            for (int i = 0; i < iterations; i++) {
                NumaConfig sncfg;
                sncfg.AllocateUsingNodePageSize = false;
                sncfg.UseThreadLocalRouting = true;
                FurrConfig scfg;
                scfg.EnableLogging = false;
                scfg.EnableNUMA = true;
                scfg.PageSize = 4096;
                scfg.InitialPageCount = 2048;
                scfg.numaConfig = &sncfg;
                FB* sfb = FB::CreateBall(dbPrefix + "Scale_" + std::to_string(tc), scfg, true);
                if (!sfb) continue;
                validIter++;
                auto sr = benchMultiThread<Policy>(sfb, "Set", tc, actualNodes, kpt, 64, true, false);
                auto gr = benchMultiThread<Policy>(sfb, "Get", tc, actualNodes, kpt, 64, false, false);
                sSum += sr.opsPerSec; gSum += gr.opsPerSec;
                delete sfb;
            }
            double avgS = (validIter > 0) ? sSum / validIter : 0;
            double avgG = (validIter > 0) ? gSum / validIter : 0;
            std::cout << std::setw(7) << tc << " | " << std::fixed << std::setprecision(0)
                      << std::setw(8) << avgS << " | " << std::setw(8) << avgG << std::endl;
        }
    }

    // --- ROUTING STRATEGY COMPARISON ---
    if (actualNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "ROUTING STRATEGY COMPARISON (" << iterations << " iterations, averaged)" << std::endl;
        std::cout << "64-byte values, 2 threads (one per node), 5000 keys/thread" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        size_t routeKeys = 5000;

        FB* fb_tl = createFB<Policy>(dbPrefix + "TL", true);
        if (fb_tl) {
            double rrSetP50 = 0, rrSetP99 = 0, rrSelfP50 = 0, rrSelfP99 = 0, rrCrossP50 = 0, rrCrossP99 = 0;
            double tlSetP50 = 0, tlSetP99 = 0, tlSelfP50 = 0, tlSelfP99 = 0, tlCrossP50 = 0, tlCrossP99 = 0;

            for (int i = 0; i < iterations; i++) {
                auto rrR = benchRoutingStrategy<Policy>(fb, "RoundRobin", actualNodes, routeKeys, 64);
                rrSetP50 += rrR.setP50; rrSetP99 += rrR.setP99;
                rrSelfP50 += rrR.selfGetP50; rrSelfP99 += rrR.selfGetP99;
                rrCrossP50 += rrR.crossGetP50; rrCrossP99 += rrR.crossGetP99;

                auto tlR = benchRoutingStrategy<Policy>(fb_tl, "ThreadLocal", actualNodes, routeKeys, 64);
                tlSetP50 += tlR.setP50; tlSetP99 += tlR.setP99;
                tlSelfP50 += tlR.selfGetP50; tlSelfP99 += tlR.selfGetP99;
                tlCrossP50 += tlR.crossGetP50; tlCrossP99 += tlR.crossGetP99;
            }

            rrSetP50 /= iterations; rrSetP99 /= iterations;
            rrSelfP50 /= iterations; rrSelfP99 /= iterations;
            rrCrossP50 /= iterations; rrCrossP99 /= iterations;
            tlSetP50 /= iterations; tlSetP99 /= iterations;
            tlSelfP50 /= iterations; tlSelfP99 /= iterations;
            tlCrossP50 /= iterations; tlCrossP99 /= iterations;

            std::cout << std::endl;
            std::cout << std::left << std::setw(20) << "Strategy" << " | "
                      << std::right << std::setw(12) << "Set p50" << " | "
                      << std::setw(12) << "Set p99" << " | "
                      << std::setw(12) << "Self-Get p50" << " | "
                      << std::setw(12) << "Self-Get p99" << " | "
                      << std::setw(12) << "Cross-Get p50" << " | "
                      << std::setw(12) << "Cross-Get p99"
                      << std::endl;
            std::cout << std::string(110, '-') << std::endl;
            std::cout << std::left << std::setw(20) << "Round-robin" << " | "
                      << std::right << std::setw(10) << std::fixed << std::setprecision(0) << rrSetP50 << " ns | "
                      << std::setw(10) << rrSetP99 << " ns | "
                      << std::setw(10) << rrSelfP50 << " ns | "
                      << std::setw(10) << rrSelfP99 << " ns | "
                      << std::setw(10) << rrCrossP50 << " ns | "
                      << std::setw(10) << rrCrossP99 << " ns"
                      << std::endl;
            std::cout << std::left << std::setw(20) << "Thread-local" << " | "
                      << std::right << std::setw(10) << std::fixed << std::setprecision(0) << tlSetP50 << " ns | "
                      << std::setw(10) << tlSetP99 << " ns | "
                      << std::setw(10) << tlSelfP50 << " ns | "
                      << std::setw(10) << tlSelfP99 << " ns | "
                      << std::setw(10) << tlCrossP50 << " ns | "
                      << std::setw(10) << tlCrossP99 << " ns"
                      << std::endl;

            double selfImprove = (rrSelfP50 > 0) ? ((rrSelfP50 - tlSelfP50) / rrSelfP50 * 100.0) : 0;
            double crossImprove = (rrCrossP50 > 0) ? ((rrCrossP50 - tlCrossP50) / rrCrossP50 * 100.0) : 0;
            std::cout << std::endl;
            std::cout << "Thread-local self-Get improvement: " << std::fixed << std::setprecision(1) << selfImprove << "%" << std::endl;
            std::cout << "Thread-local cross-Get improvement: " << std::fixed << std::setprecision(1) << crossImprove << "%" << std::endl;
            std::cout << "Thread-local optimistic hit rate: " << std::fixed << std::setprecision(1)
                      << (fb_tl->Stats.GetHitCount() > 0 ? (100.0 * fb_tl->Stats.GetLocalHitCount() / fb_tl->Stats.GetHitCount()) : 0) << "%" << std::endl;

            delete fb_tl;
        } else {
            std::cout << "Failed to create thread-local routing FurrBall" << std::endl;
        }
    }

    // --- ZIPFIAN WORKLOAD ---
    if (actualNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "ZIPFIAN WORKLOAD (theta=0.99, " << iterations << " iterations)" << std::endl;
        std::cout << "Each thread populates local keys, then reads using Zipfian selection." << std::endl;
        std::cout << "Compares round-robin vs thread-local under skewed access." << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        auto zipfianSample = [](size_t n, double theta, std::mt19937_64& rng) -> size_t {
            if (n == 0) return 0;
            double alpha = 1.0 / (1.0 - theta);
            double zeta = 0.0;
            for (size_t i = 0; i < n; i++) zeta += 1.0 / std::pow((double)(i + 1), theta);
            double eta = (1.0 - std::pow(2.0 / (double)n, 1.0 - theta)) / (1.0 - zeta + std::pow(2.0 / (double)n, 1.0 - theta) / (1.0 - (1.0 / (double)n)));
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            double u = dist(rng);
            double uz = u * zeta;
            if (uz < 1.0) return 0;
            if (uz < 1.0 + std::pow(0.5, theta)) return 1;
            return (size_t)((double)n * std::pow(eta * u - eta + 1.0, 1.0 / (1.0 - theta))) % n;
        };

        size_t zipfKeysPerThread = 5000;
        size_t zipfReadOps = 10000;

        auto benchZipfian = [&](FB* fball, const std::string& name, int nNodes) -> void {
            std::cout << std::endl;
            std::cout << "--- " << name << " ---" << std::endl;

            struct ZipfThreadResult {
                std::vector<ns> selfGetLatencies;
                std::vector<ns> crossGetLatencies;
                size_t localHits = 0;
                size_t totalGets = 0;
            };

            double selfP50Sum = 0, selfP99Sum = 0, crossP50Sum = 0, crossP99Sum = 0;
            double localHitRateSum = 0;
            unsigned int totalHitsBefore = fball->Stats.GetHitCount();
            unsigned int totalLocalHitsBefore = fball->Stats.GetLocalHitCount();

            for (int iter = 0; iter < iterations; iter++) {
                std::vector<ZipfThreadResult> threadResults(nNodes);
                std::vector<std::thread> threads(nNodes);

                auto worker = [&](int nodeId, ZipfThreadResult& tr) {
                    Numatic::PinCurrentThreadToNode(nodeId);
                    std::mt19937_64 rng(nodeId * 1000 + iter);
                    std::vector<char> value(64, 'X');
                    std::vector<char> outBuf(128);

                    for (size_t i = 0; i < zipfKeysPerThread; i++) {
                        std::string key = "zipf_" + std::string(policyName) + "_t" + std::to_string(nodeId) + "_" + std::to_string(i);
                        fball->Set(key, value.data(), 64);
                    }

                    int otherNode = (nodeId + 1) % nNodes;

                    tr.selfGetLatencies.resize(zipfReadOps);
                    tr.crossGetLatencies.resize(zipfReadOps);
                    tr.localHits = 0;
                    tr.totalGets = zipfReadOps * 2;

                    for (size_t i = 0; i < zipfReadOps; i++) {
                        size_t selfIdx = zipfianSample(zipfKeysPerThread, 0.99, rng);
                        std::string selfKey = "zipf_" + std::string(policyName) + "_t" + std::to_string(nodeId) + "_" + std::to_string(selfIdx);
                        size_t outSize = 0;
                        auto t0 = Clock::now();
                        fball->Get(selfKey, outBuf.data(), outBuf.size(), outSize);
                        tr.selfGetLatencies[i] = Clock::now() - t0;
                    }

                    for (size_t i = 0; i < zipfReadOps; i++) {
                        size_t crossIdx = zipfianSample(zipfKeysPerThread, 0.99, rng);
                        std::string crossKey = "zipf_" + std::string(policyName) + "_t" + std::to_string(otherNode) + "_" + std::to_string(crossIdx);
                        size_t outSize = 0;
                        auto t0 = Clock::now();
                        fball->Get(crossKey, outBuf.data(), outBuf.size(), outSize);
                        tr.crossGetLatencies[i] = Clock::now() - t0;
                    }
                };

                for (int t = 0; t < nNodes; t++)
                    threads[t] = std::thread(worker, t, std::ref(threadResults[t]));
                for (auto& th : threads) th.join();

                std::vector<ns> allSelf, allCross;
                for (auto& tr : threadResults) {
                    allSelf.insert(allSelf.end(), tr.selfGetLatencies.begin(), tr.selfGetLatencies.end());
                    allCross.insert(allCross.end(), tr.crossGetLatencies.begin(), tr.crossGetLatencies.end());
                }

                auto agg = [](std::vector<ns>& v) -> std::tuple<double, double> {
                    if (v.empty()) return {0, 0};
                    std::sort(v.begin(), v.end());
                    return {v[v.size() / 2].count(), v[v.size() * 99 / 100].count()};
                };

                auto [sp50, sp99] = agg(allSelf);
                auto [cp50, cp99] = agg(allCross);
                selfP50Sum += sp50; selfP99Sum += sp99;
                crossP50Sum += cp50; crossP99Sum += cp99;
            }

            unsigned int totalHits = fball->Stats.GetHitCount() - totalHitsBefore;
            unsigned int totalLocalHits = fball->Stats.GetLocalHitCount() - totalLocalHitsBefore;
            double optimisticRate = (totalHits > 0) ? (100.0 * totalLocalHits / totalHits) : 0;

            std::cout << std::left << std::setw(20) << name << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << "Self-Get p50 " << std::setw(8) << selfP50Sum / iterations << " ns | "
                      << "Self-Get p99 " << std::setw(8) << selfP99Sum / iterations << " ns | "
                      << "Cross-Get p50 " << std::setw(8) << crossP50Sum / iterations << " ns | "
                      << "Cross-Get p99 " << std::setw(8) << crossP99Sum / iterations << " ns"
                      << std::endl;
            std::cout << std::left << std::setw(20) << "" << " | "
                      << "Optimistic hit rate: " << std::fixed << std::setprecision(1) << optimisticRate << "%"
                      << " | Cross-node overhead (p50): " << std::fixed << std::setprecision(1)
                      << ((selfP50Sum > 0) ? ((crossP50Sum / iterations - selfP50Sum / iterations) / (selfP50Sum / iterations) * 100.0) : 0) << "%"
                      << std::endl;
        };

        FB* fb_tl = createFB<Policy>(dbPrefix + "ZF_TL", true);
        if (fb_tl) {
            benchZipfian(fb, "Round-robin", actualNodes);
            benchZipfian(fb_tl, "Thread-local", actualNodes);
            delete fb_tl;
        } else {
            std::cout << "Failed to create thread-local FurrBall for Zipfian test" << std::endl;
        }
    }

    std::cout << std::endl;
    std::cout << "Cache Stats (" << policyName << "):" << std::endl;
    printStats(fb);

    delete fb;
    std::cout << std::endl;
}

static void runArchBenchmarks(int numNodes, int iterations) {
    using FB = FurrBall<StandardRemarc>;

    // --- BASELINE COMPARISON ---
    if (numNodes >= 2) {
        std::cout << std::string(120, '#') << std::endl;
        std::cout << "ARCHITECTURE: Baseline + Variant + Ablation (policy-independent)" << std::endl;
        std::cout << std::string(120, '#') << std::endl;

        std::cout << std::string(110, '=') << std::endl;
        std::cout << "BASELINE COMPARISON: Furrballs vs Non-NUMA Cache (" << iterations << " iterations)" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        FB* fb_fresh = FB::CreateBall("BenchDB_BL", []{
            NumaConfig nc; nc.AllocateUsingNodePageSize = false;
            FurrConfig fc; fc.EnableLogging = false; fc.EnableNUMA = true;
            fc.PageSize = 4096; fc.InitialPageCount = 2048; fc.numaConfig = &nc;
            return fc;
        }());

        BaselineCache baseline(4096, 4096);

        auto benchBaselineST = [&](bool doSet, size_t numOps, size_t valueSize) -> BenchResult {
            BenchResult r;
            r.name = doSet ? "BL Set" : "BL Get";
            r.ops = numOps;
            std::vector<char> val(valueSize, 'X');
            std::vector<ns> lat(numOps);
            auto start = Clock::now();
            for (size_t i = 0; i < numOps; i++) {
                std::string k = "bl_" + std::to_string(i);
                if (doSet) { auto t0 = Clock::now(); baseline.Set(k, val.data(), valueSize); lat[i] = Clock::now()-t0; }
                else { size_t os=0; auto t0 = Clock::now(); baseline.Get(k, val.data(), valueSize, os); lat[i] = Clock::now()-t0; }
            }
            auto end = Clock::now();
            r.durationMs = std::chrono::duration<double,std::milli>(end-start).count();
            r.opsPerSec = (r.durationMs>0) ? numOps/(r.durationMs/1000.0) : 0;
            std::sort(lat.begin(), lat.end());
            double tot=0; for(auto&l:lat) tot+=l.count();
            r.avgLatencyNs = tot/numOps;
            r.p50Ns = lat[numOps/2].count(); r.p99Ns = lat[numOps*99/100].count();
            return r;
        };

        if (fb_fresh) {
            auto bSet = benchBaselineST(true, 10000, 64);
            auto bGet = benchBaselineST(false, 10000, 64);

            auto fSet = runIterations<StandardRemarc>(fb_fresh, [&](FB*) -> BenchResult {
                return benchSingleThread<StandardRemarc>(fb_fresh, "bl64", 10000, 64, true);
            }, iterations);
            auto fGet = runIterations<StandardRemarc>(fb_fresh, [&](FB* b){ return benchSingleThread<StandardRemarc>(b, "bl64", 10000, 64, false); }, iterations);

            std::cout << std::left << std::setw(20) << "Baseline (no NUMA)" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << "Set p50 " << std::setw(8) << bSet.p50Ns << " ns | "
                      << "Get p50 " << std::setw(8) << bGet.p50Ns << " ns"
                      << std::endl;
            std::cout << std::left << std::setw(20) << "Furrballs" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << "Set p50 " << std::setw(8) << fSet.avgP50 << " ns | "
                      << "Get p50 " << std::setw(8) << fGet.avgP50 << " ns"
                      << std::endl;

            double setImprove = (bSet.p50Ns > 0) ? ((bSet.p50Ns - fSet.avgP50) / bSet.p50Ns * 100.0) : 0;
            double getImprove = (bGet.p50Ns > 0) ? ((bGet.p50Ns - fGet.avgP50) / bGet.p50Ns * 100.0) : 0;
            std::cout << "Furrballs improvement: Set " << std::fixed << std::setprecision(1) << setImprove
                      << "%, Get " << getImprove << "%" << std::endl;

            // Concurrent throughput
            std::cout << std::endl;
            std::cout << "Concurrent throughput (4 threads, 2500 keys/thread, 64B):" << std::endl;
            {
                size_t keysPT = 2500;
                double blSetP50=0, blGetP50=0, fbSetP50=0, fbGetP50=0;
                for (int it=0; it<iterations; it++) {
                    std::vector<std::thread> thr(numNodes);
                    std::vector<std::vector<ns>> sLats(numNodes), gLats(numNodes);
                    std::vector<char> val(64,'X'), out(128);
                    for (int t=0;t<numNodes;t++){
                        thr[t]=std::thread([&,t](){
                            Numatic::PinCurrentThreadToNode(t);
                            for(size_t i=0;i<keysPT;i++){
                                std::string k="blmt_t"+std::to_string(t)+"_"+std::to_string(i);
                                auto t0=Clock::now(); baseline.Set(k,val.data(),64);
                                sLats[t].push_back(Clock::now()-t0);
                                size_t os=0;
                                t0=Clock::now(); baseline.Get(k,out.data(),out.size(),os);
                                gLats[t].push_back(Clock::now()-t0);
                            }
                        });
                    }
                    for(auto&th:thr) th.join();
                    std::vector<ns> flatS, flatG;
                    for(auto&v:sLats) flatS.insert(flatS.end(),v.begin(),v.end());
                    for(auto&v:gLats) flatG.insert(flatG.end(),v.begin(),v.end());
                    std::sort(flatS.begin(),flatS.end());
                    std::sort(flatG.begin(),flatG.end());
                    blSetP50+=flatS[flatS.size()/2].count();
                    blGetP50+=flatG[flatG.size()/2].count();
                }
                auto fSetR = benchMultiThread<StandardRemarc>(fb_fresh,"Set",4,numNodes,keysPT,64,true,false);
                auto fGetR = benchMultiThread<StandardRemarc>(fb_fresh,"Get",4,numNodes,keysPT,64,false,false);

                std::cout << std::left << std::setw(20) << "Baseline (no NUMA)" << " | "
                          << std::right << std::fixed << std::setprecision(0)
                          << "Set p50 " << std::setw(8) << blSetP50/iterations << " ns | "
                          << "Get p50 " << std::setw(8) << blGetP50/iterations << " ns"
                          << std::endl;
                std::cout << std::left << std::setw(20) << "Furrballs" << " | "
                          << std::right << std::fixed << std::setprecision(0)
                          << "Set p50 " << std::setw(8) << fSetR.p50Ns << " ns | "
                          << "Get p50 " << std::setw(8) << fGetR.p50Ns << " ns"
                          << std::endl;
            }

            delete fb_fresh;
        }
    }

    // --- VARIANT COMPARISON: Furrballs vs SharedNothingCache ---
    if (numNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "VARIANT COMPARISON: Shared+SeqLock vs Shared-Nothing (MPSC queue)" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        using namespace NuAtlas::SM;
        SM::SMConfig smCfg;
        smCfg.PageSize = 4096;
        smCfg.InitialPageCount = 2048;

        SharedNothingCache* sm = SharedNothingCache::Create("BenchDB_SM", smCfg);
        if (!sm) {
            std::cout << "Failed to create SharedNothingCache" << std::endl;
        } else {
            auto benchVariant = [&](auto& cache, const std::string& name, const std::string& prefix, bool isSM) -> void {
                std::cout << std::endl;
                std::cout << "--- " << name << " ---" << std::endl;

                size_t numOps = 10000;
                std::vector<char> val(64, 'X');
                std::vector<char> outBuf(128);

                double setP50 = 0, getP50 = 0, selfGetP50 = 0, crossGetP50 = 0;
                double localHitRate = 0;

                for (int iter = 0; iter < iterations; iter++) {
                    {
                        std::vector<ns> lat(numOps);
                        for (size_t i = 0; i < numOps; i++) {
                            std::string key = prefix + "_key_" + std::to_string(i);
                            auto t0 = Clock::now();
                            cache.Set(key, val.data(), 64);
                            lat[i] = Clock::now() - t0;
                        }
                        std::sort(lat.begin(), lat.end());
                        setP50 += lat[numOps / 2].count();
                    }
                    {
                        std::vector<ns> lat(numOps);
                        for (size_t i = 0; i < numOps; i++) {
                            std::string key = prefix + "_key_" + std::to_string(i);
                            size_t outSize = 0;
                            auto t0 = Clock::now();
                            cache.Get(key, outBuf.data(), outBuf.size(), outSize);
                            lat[i] = Clock::now() - t0;
                        }
                        std::sort(lat.begin(), lat.end());
                        getP50 += lat[numOps / 2].count();
                    }
                }
                setP50 /= iterations;
                getP50 /= iterations;

                {
                    size_t keysPerThread = 5000;
                    double sp50 = 0, cp50 = 0;
                    for (int iter = 0; iter < iterations; iter++) {
                        std::vector<std::thread> threads(numNodes);
                        struct TResult { std::vector<ns> self, cross; };
                        std::vector<TResult> results(numNodes);

                        for (int t = 0; t < numNodes; t++) {
                            threads[t] = std::thread([&, t]() {
                                Numatic::PinCurrentThreadToNode(t);
                                std::vector<char> v(64, 'Y');
                                for (size_t i = 0; i < keysPerThread; i++) {
                                    cache.Set(prefix + "_mt_t" + std::to_string(t) + "_" + std::to_string(i), v.data(), 64);
                                }
                                results[t].self.resize(keysPerThread);
                                results[t].cross.resize(keysPerThread);
                                int other = (t + 1) % numNodes;
                                for (size_t i = 0; i < keysPerThread; i++) {
                                    std::string sk = prefix + "_mt_t" + std::to_string(t) + "_" + std::to_string(i);
                                    size_t os = 0;
                                    auto t0 = Clock::now();
                                    cache.Get(sk, outBuf.data(), outBuf.size(), os);
                                    results[t].self[i] = Clock::now() - t0;
                                }
                                for (size_t i = 0; i < keysPerThread; i++) {
                                    std::string ck = prefix + "_mt_t" + std::to_string(other) + "_" + std::to_string(i);
                                    size_t os = 0;
                                    auto t0 = Clock::now();
                                    cache.Get(ck, outBuf.data(), outBuf.size(), os);
                                    results[t].cross[i] = Clock::now() - t0;
                                }
                            });
                        }
                        for (auto& th : threads) th.join();

                        std::vector<ns> allSelf, allCross;
                        for (auto& r : results) {
                            allSelf.insert(allSelf.end(), r.self.begin(), r.self.end());
                            allCross.insert(allCross.end(), r.cross.begin(), r.cross.end());
                        }
                        std::sort(allSelf.begin(), allSelf.end());
                        std::sort(allCross.begin(), allCross.end());
                        sp50 += allSelf[allSelf.size() / 2].count();
                        cp50 += allCross[allCross.size() / 2].count();
                    }
                    selfGetP50 = sp50 / iterations;
                    crossGetP50 = cp50 / iterations;
                }

                unsigned int hits = 0, localHits = 0;
                if constexpr (std::is_same_v<std::decay_t<decltype(cache)>, SharedNothingCache>) {
                    hits = cache.Stats.GetHitCount();
                    localHits = cache.Stats.GetLocalHitCount();
                } else {
                    hits = cache.Stats.GetHitCount();
                    localHits = cache.Stats.GetLocalHitCount();
                }
                double optRate = (hits > 0) ? (100.0 * localHits / hits) : 0;

                std::cout << std::fixed << std::setprecision(0);
                std::cout << "  ST Set p50: " << std::setw(8) << setP50 << " ns" << std::endl;
                std::cout << "  ST Get p50: " << std::setw(8) << getP50 << " ns" << std::endl;
                std::cout << "  MT Self-Get p50: " << std::setw(8) << selfGetP50 << " ns" << std::endl;
                std::cout << "  MT Cross-Get p50: " << std::setw(8) << crossGetP50 << " ns" << std::endl;
                double crossOverhead = (selfGetP50 > 0) ? ((crossGetP50 - selfGetP50) / selfGetP50 * 100.0) : 0;
                std::cout << "  Cross-node overhead: " << std::fixed << std::setprecision(1) << crossOverhead << "%" << std::endl;
                std::cout << "  Optimistic hit rate: " << std::fixed << std::setprecision(1) << optRate << "%" << std::endl;
            };

            NumaConfig numaSM;
            numaSM.AllocateUsingNodePageSize = false;
            FurrConfig cfgSM;
            cfgSM.EnableLogging = false;
            cfgSM.EnableNUMA = true;
            cfgSM.PageSize = 4096;
            cfgSM.InitialPageCount = 2048;
            cfgSM.numaConfig = &numaSM;

            FurrBall<StandardRemarc>* fb_sm = FurrBall<StandardRemarc>::CreateBall("BenchDB_SM_FB", cfgSM);
            if (fb_sm) {
                benchVariant(*fb_sm, "Furrballs (shared+SeqLock)", "smfb", false);
                delete fb_sm;
            }
            benchVariant(*sm, "SharedNothing (MPSC queue)", "smsm", true);

            delete sm;
        }
    }

    // --- ABLATION STUDY ---
    if (numNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "ABLATION STUDY: Isolating each design decision's contribution" << std::endl;
        std::cout << "Each step adds one architectural change on top of the previous." << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        size_t numOps = 10000;
        std::vector<char> val(64, 'X');
        std::vector<char> outBuf(128);

        auto benchAblation = [&](auto& cache, const std::string& name, const std::string& prefix) -> void {
            double setP50 = 0, getP50 = 0;
            double selfP50 = 0, crossP50 = 0;
            size_t keysPerThread = 5000;

            for (int iter = 0; iter < iterations; iter++) {
                {
                    std::vector<ns> lat(numOps);
                    for (size_t i = 0; i < numOps; i++) {
                        auto t0 = Clock::now();
                        cache.Set(prefix + "_" + std::to_string(i), val.data(), 64);
                        lat[i] = Clock::now() - t0;
                    }
                    std::sort(lat.begin(), lat.end());
                    setP50 += lat[numOps / 2].count();
                }
                {
                    std::vector<ns> lat(numOps);
                    for (size_t i = 0; i < numOps; i++) {
                        size_t os = 0;
                        auto t0 = Clock::now();
                        cache.Get(prefix + "_" + std::to_string(i), outBuf.data(), outBuf.size(), os);
                        lat[i] = Clock::now() - t0;
                    }
                    std::sort(lat.begin(), lat.end());
                    getP50 += lat[numOps / 2].count();
                }
            }
            setP50 /= iterations;
            getP50 /= iterations;

            {
                double sp = 0, cp = 0;

                for (int iter = 0; iter < iterations; iter++) {
                    std::vector<std::thread> threads(numNodes);
                    struct R { std::vector<ns> self, cross; };
                    std::vector<R> results(numNodes);

                    for (int t = 0; t < numNodes; t++) {
                        threads[t] = std::thread([&, t]() {
                            Numatic::PinCurrentThreadToNode(t);
                            for (size_t i = 0; i < keysPerThread; i++)
                                cache.Set(prefix + "_ab_t" + std::to_string(t) + "_" + std::to_string(i), val.data(), 64);
                            results[t].self.resize(keysPerThread);
                            results[t].cross.resize(keysPerThread);
                            int other = (t + 1) % numNodes;
                            for (size_t i = 0; i < keysPerThread; i++) {
                                size_t os = 0;
                                auto t0 = Clock::now();
                                cache.Get(prefix + "_ab_t" + std::to_string(t) + "_" + std::to_string(i), outBuf.data(), outBuf.size(), os);
                                results[t].self[i] = Clock::now() - t0;
                            }
                            for (size_t i = 0; i < keysPerThread; i++) {
                                size_t os = 0;
                                auto t0 = Clock::now();
                                cache.Get(prefix + "_ab_t" + std::to_string(other) + "_" + std::to_string(i), outBuf.data(), outBuf.size(), os);
                                results[t].cross[i] = Clock::now() - t0;
                            }
                        });
                    }
                    for (auto& th : threads) th.join();
                    std::vector<ns> allSelf, allCross;
                    for (auto& r : results) {
                        allSelf.insert(allSelf.end(), r.self.begin(), r.self.end());
                        allCross.insert(allCross.end(), r.cross.begin(), r.cross.end());
                    }
                    std::sort(allSelf.begin(), allSelf.end());
                    std::sort(allCross.begin(), allCross.end());
                    sp += allSelf[allSelf.size()/2].count();
                    cp += allCross[allCross.size()/2].count();
                }
                selfP50 = sp / iterations;
                crossP50 = cp / iterations;
            }

            double crossOH = (selfP50 > 0) ? ((crossP50 - selfP50) / selfP50 * 100.0) : 0;
            std::cout << std::left << std::setw(36) << name << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << "Set " << std::setw(6) << setP50 << " | "
                      << "Get " << std::setw(6) << getP50 << " | "
                      << "Self " << std::setw(6) << selfP50 << " | "
                      << "Cross " << std::setw(6) << crossP50 << " | "
                      << "X-OH " << std::setprecision(1) << std::setw(6) << crossOH << "%"
                      << std::endl;
        };

        std::cout << std::endl;
        std::cout << std::left << std::setw(36) << "Configuration" << " | "
                  << std::right << "  ST Set  |    ST Get |   MT Self |  MT Cross | Cross-OH" << std::endl;
        std::cout << std::string(110, '-') << std::endl;

        BaselineCache baseline(4096, 4096);
        benchAblation(baseline, "A: Baseline (malloc, 1 map)", "abA");

        NUMAAllocCache numaAlloc(4096, 4096, numNodes);
        benchAblation(numaAlloc, "B: + NUMA alloc (1 map)", "abB");

        NumaConfig numaRR;
        numaRR.AllocateUsingNodePageSize = false;
        FurrConfig cfgRR;
        cfgRR.EnableLogging = false;
        cfgRR.EnableNUMA = true;
        cfgRR.PageSize = 4096;
        cfgRR.InitialPageCount = 2048;
        cfgRR.numaConfig = &numaRR;
        FurrBall<StandardRemarc>* fb_rr = FurrBall<StandardRemarc>::CreateBall("BenchDB_AB_C", cfgRR);
        if (fb_rr) {
            benchAblation(*fb_rr, "C: + Per-node sharding", "abC");

            NumaConfig numaTL2;
            numaTL2.AllocateUsingNodePageSize = false;
            numaTL2.UseThreadLocalRouting = true;
            FurrConfig cfgTL2;
            cfgTL2.EnableLogging = false;
            cfgTL2.EnableNUMA = true;
            cfgTL2.PageSize = 4096;
            cfgTL2.InitialPageCount = 2048;
            cfgTL2.numaConfig = &numaTL2;
            FurrBall<StandardRemarc>* fb_tl2 = FurrBall<StandardRemarc>::CreateBall("BenchDB_AB_D", cfgTL2);
            if (fb_tl2) {
                benchAblation(*fb_tl2, "D: + Thread-local routing", "abD");
                delete fb_tl2;
            }
            delete fb_rr;
        }

        using namespace NuAtlas::SM;
        SM::SMConfig smCfg2;
        smCfg2.PageSize = 4096;
        smCfg2.InitialPageCount = 2048;
        SharedNothingCache* sm2 = SharedNothingCache::Create("BenchDB_AB_E", smCfg2);
        if (sm2) {
            benchAblation(*sm2, "E: + Shared-nothing (MPSC)", "abE");
            delete sm2;
        }
    }
}

int main() {
    FurrBall<StandardRemarc>::Bootstrap();

    int numNodes = Numatic::GetNodeCount();
    int iterations = 10;
    std::cout << "=== Furrballs Multi-Policy Benchmark ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << std::endl;
    std::cout << "NUMA page size: " << Numatic::GetNodePageSize() << std::endl;
#ifdef SIMULATE_NUMA_LATENCY_NS
    std::cout << "NUMA latency simulation: " << SIMULATE_NUMA_LATENCY_NS << " ns (cross-node)" << std::endl;
#else
    std::cout << "NUMA latency simulation: disabled" << std::endl;
#endif
    std::cout << "Iterations per test: " << iterations << std::endl;
    std::cout << "Policies: ARC, StandardRemarc, AugAdapt" << std::endl;
    std::cout << std::endl;

    runPolicyBenchmarks<ArcPolicy>("ARC", numNodes, iterations);
    runPolicyBenchmarks<StandardRemarc>("REMARC", numNodes, iterations);
    runPolicyBenchmarks<AugAdaptPolicy>("AUG-ADAPT", numNodes, iterations);
    runArchBenchmarks(numNodes, iterations);

    FurrBall<StandardRemarc>::Shutdown();
    return 0;
}
