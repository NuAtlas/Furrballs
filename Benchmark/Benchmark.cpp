#include "Furrballs.h"
#include "Numatic.h"
#include "BaselineCache.h"
#include "SharedNothingCache.h"
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

struct ThreadArg {
    FurrBall* fb;
    size_t startKey;
    size_t endKey;
    size_t valueSize;
    int pinNode;
    bool doSet;
    std::vector<ns>* latencies;
};

static void threadWorker(ThreadArg* arg) {
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

static void warmupSet(FurrBall* fb, const std::string& prefix, size_t numOps, size_t valueSize) {
    std::vector<char> value(valueSize, 'X');
    for (size_t i = 0; i < numOps; i++) {
        std::string key = prefix + "_key_" + std::to_string(i);
        fb->Set(key, value.data(), valueSize);
    }
}

static void warmupSetMT(FurrBall* fb, int numThreads, size_t keysPerThread, size_t valueSize, bool crossNode, int numNodes) {
    std::vector<ThreadArg> args(numThreads);
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
        threads[t] = std::thread(threadWorker, &args[t]);
    for (auto& th : threads) th.join();
}

static BenchResult benchSingleThread(FurrBall* fb, const std::string& prefix, size_t numOps, size_t valueSize, bool doSet) {
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

static BenchResult benchMultiThread(FurrBall* fb, const std::string& name, int numThreads, int numNodes, size_t keysPerThread, size_t valueSize, bool doSet, bool crossNode) {
    BenchResult result;
    result.name = name;

    std::vector<ThreadArg> args(numThreads);
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
        threads[t] = std::thread(threadWorker, &args[t]);
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

static IterationResult runIterations(FurrBall* fb, const std::function<BenchResult(FurrBall*)>& benchFn, int iterations) {
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

static RoutingBenchResult benchRoutingStrategy(FurrBall* fb, const std::string& name, int numNodes, size_t keysPerThread, size_t valueSize) {
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

int main() {
    FurrBall::Bootstrap();

    int numNodes = Numatic::GetNodeCount();
    int iterations = 5;
    std::cout << "=== Furrballs Benchmark ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << std::endl;
    std::cout << "NUMA page size: " << Numatic::GetNodePageSize() << std::endl;
#ifdef SIMULATE_NUMA_LATENCY_NS
    std::cout << "NUMA latency simulation: " << SIMULATE_NUMA_LATENCY_NS << " ns (cross-node)" << std::endl;
#else
    std::cout << "NUMA latency simulation: disabled" << std::endl;
#endif
    std::cout << "Iterations per test: " << iterations << std::endl;
    std::cout << std::endl;

    NumaConfig numaConfig;
    numaConfig.AllocateUsingNodePageSize = false;

    FurrConfig config;
    config.EnableLogging = false;
    config.EnableNUMA = true;
    config.PageSize = 4096;
    config.InitialPageCount = 2048;
    config.numaConfig = &numaConfig;

    FurrBall* fb = FurrBall::CreateBall("BenchDB", config);
    if (!fb) {
        config.EnableNUMA = false;
        config.numaConfig = nullptr;
        fb = FurrBall::CreateBall("BenchDB", config);
        numNodes = 1;
    }
    if (!fb) {
        std::cerr << "Cannot create FurrBall" << std::endl;
        FurrBall::Shutdown();
        return -1;
    }

    // --- SINGLE-THREADED ---
    std::cout << std::string(110, '=') << std::endl;
    std::cout << "SINGLE-THREADED (" << iterations << " iterations, averaged)" << std::endl;
    std::cout << std::string(110, '=') << std::endl;

    std::cout << std::string(110, '-') << std::endl;
    std::cout << "64-byte values, 10000 ops" << std::endl;
    std::cout << std::string(110, '-') << std::endl;
    {
        auto setI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "64B", 10000, 64, true); }, iterations);
        auto getI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "64B", 10000, 64, false); }, iterations);
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
        auto setI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "512B", 5000, 512, true); }, iterations);
        auto getI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "512B", 5000, 512, false); }, iterations);
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
        auto setI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "4KB", 1000, 4096, true); }, iterations);
        auto getI = runIterations(fb, [&](FurrBall* b){ return benchSingleThread(b, "4KB", 1000, 4096, false); }, iterations);
        std::cout << "Set  | " << std::fixed << std::setprecision(0)
                  << "avg " << setI.avgOpsPerSec << " ops/s | p50 " << setI.avgP50 << " ns | p99 " << setI.avgP99 << " ns | stddev " << setI.avgStddev << " ns" << std::endl;
        std::cout << "Get  | " << std::fixed << std::setprecision(0)
                  << "avg " << getI.avgOpsPerSec << " ops/s | p50 " << getI.avgP50 << " ns | p99 " << getI.avgP99 << " ns | stddev " << getI.avgStddev << " ns" << std::endl;
    }
    std::cout << std::endl;

    // --- MULTI-THREADED ---
    if (numNodes >= 2) {
        // 2 threads
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
                auto getL = benchMultiThread(fb, "Get local", 2, numNodes, 5000, 64, false, false);
                auto getX = benchMultiThread(fb, "Get cross", 2, numNodes, 5000, 64, false, true);
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
                auto getL = benchMultiThread(fb, "Get local", 2, numNodes, 2000, 512, false, false);
                auto getX = benchMultiThread(fb, "Get cross", 2, numNodes, 2000, 512, false, true);
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
                auto setR = benchMultiThread(fb, "Set", 4, numNodes, 2500, 64, true, false);
                auto getR = benchMultiThread(fb, "Get", 4, numNodes, 2500, 64, false, false);
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
                auto setR = benchMultiThread(fb, "Set", 4, numNodes, 2500, 64, true, true);
                auto getR = benchMultiThread(fb, "Get", 4, numNodes, 2500, 64, false, true);
                setOpsSum += setR.opsPerSec;
                getOpsSum += getR.opsPerSec;
            }
            std::cout << "Set (4t cross) | " << std::fixed << std::setprecision(0)
                      << "avg " << setOpsSum / iterations << " ops/s" << std::endl;
            std::cout << "Get (4t cross) | " << std::fixed << std::setprecision(0)
                      << "avg " << getOpsSum / iterations << " ops/s" << std::endl;
        }
    } else {
        std::cout << "Skipping multi-threaded NUMA benchmarks (1 node). Run in QEMU VM for NUMA results." << std::endl;
    }

    // --- ROUTING STRATEGY COMPARISON ---
    if (numNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "ROUTING STRATEGY COMPARISON (" << iterations << " iterations, averaged)" << std::endl;
        std::cout << "64-byte values, 2 threads (one per node), 5000 keys/thread" << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        size_t routeKeys = 5000;

        NumaConfig numaTL;
        numaTL.AllocateUsingNodePageSize = false;
        numaTL.UseThreadLocalRouting = true;

        FurrConfig configTL;
        configTL.EnableLogging = false;
        configTL.EnableNUMA = true;
        configTL.PageSize = 4096;
        configTL.InitialPageCount = 2048;
        configTL.numaConfig = &numaTL;

        FurrBall* fb_tl = FurrBall::CreateBall("BenchDB_TL", configTL);
        if (fb_tl) {
            double rrSetP50 = 0, rrSetP99 = 0, rrSelfP50 = 0, rrSelfP99 = 0, rrCrossP50 = 0, rrCrossP99 = 0;
            double tlSetP50 = 0, tlSetP99 = 0, tlSelfP50 = 0, tlSelfP99 = 0, tlCrossP50 = 0, tlCrossP99 = 0;

            for (int i = 0; i < iterations; i++) {
                auto rrR = benchRoutingStrategy(fb, "RoundRobin", numNodes, routeKeys, 64);
                rrSetP50 += rrR.setP50; rrSetP99 += rrR.setP99;
                rrSelfP50 += rrR.selfGetP50; rrSelfP99 += rrR.selfGetP99;
                rrCrossP50 += rrR.crossGetP50; rrCrossP99 += rrR.crossGetP99;

                auto tlR = benchRoutingStrategy(fb_tl, "ThreadLocal", numNodes, routeKeys, 64);
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

    // --- BASELINE COMPARISON ---
    if (numNodes >= 2) {
        std::cout << std::string(110, '=') << std::endl;
        std::cout << "BASELINE COMPARISON: Furrballs vs Non-NUMA Cache (" << iterations << " iterations)" << std::endl;
        std::cout << "Same SeqLock reads, same bump allocator. Difference: per-node allocation + sharding." << std::endl;
        std::cout << std::string(110, '=') << std::endl;

        FurrBall* fb_fresh = FurrBall::CreateBall("BenchDB_BL", config);
        if (!fb_fresh) fb_fresh = fb;

        BaselineCache baseline(4096, 4096);

        auto benchBaselineST = [&](bool doSet, size_t numOps, size_t valueSize) -> IterationResult {
            return runIterations(fb_fresh, [&](FurrBall*) -> BenchResult {
                BenchResult r;
                r.name = doSet ? "Set" : "Get";
                r.ops = numOps;
                std::vector<char> value(valueSize, 'X');
                std::vector<char> outBuf(valueSize + 64);
                std::vector<ns> latencies(numOps);

                if (!doSet) {
                    for (size_t i = 0; i < numOps; i++)
                        baseline.Set("bl_" + std::to_string(i), value.data(), valueSize);
                }

                for (size_t i = 0; i < numOps; i++) {
                    std::string key = "bl_" + std::to_string(i);
                    if (doSet) {
                        auto t0 = Clock::now();
                        baseline.Set(key, value.data(), valueSize);
                        latencies[i] = Clock::now() - t0;
                    } else {
                        size_t outSize = 0;
                        auto t0 = Clock::now();
                        baseline.Get(key, outBuf.data(), outBuf.size(), outSize);
                        latencies[i] = Clock::now() - t0;
                    }
                }

                std::sort(latencies.begin(), latencies.end());
                double totalNs = 0;
                for (auto& l : latencies) totalNs += l.count();
                r.durationMs = totalNs / 1e6;
                r.opsPerSec = (r.durationMs > 0) ? (numOps / (r.durationMs / 1000.0)) : 0;
                r.avgLatencyNs = totalNs / numOps;
                r.p50Ns = latencies[numOps / 2].count();
                r.p99Ns = latencies[numOps * 99 / 100].count();
                r.stddevNs = computeStddev(latencies, r.avgLatencyNs);
                return r;
            }, iterations);
        };

        {
            auto fSet = runIterations(fb_fresh, [&](FurrBall* b){ return benchSingleThread(b, "bl64", 10000, 64, true); }, iterations);
            auto bSet = benchBaselineST(true, 10000, 64);
            auto fGet = runIterations(fb_fresh, [&](FurrBall* b){ return benchSingleThread(b, "bl64", 10000, 64, false); }, iterations);
            auto bGet = benchBaselineST(false, 10000, 64);

            std::cout << std::endl;
            std::cout << "Single-threaded 64B (5 iterations averaged):" << std::endl;
            std::cout << std::left << std::setw(20) << "Implementation" << " | "
                      << std::right << std::setw(14) << "Set ops/s" << " | "
                      << std::setw(10) << "Set p50" << " | "
                      << std::setw(14) << "Get ops/s" << " | "
                      << std::setw(10) << "Get p50" << " | "
                      << std::setw(10) << "Get p99"
                      << std::endl;
            std::cout << std::string(110, '-') << std::endl;
            std::cout << std::left << std::setw(20) << "Furrballs" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << std::setw(14) << fSet.avgOpsPerSec << " | "
                      << std::setw(8) << fSet.avgP50 << " ns | "
                      << std::setw(14) << fGet.avgOpsPerSec << " | "
                      << std::setw(8) << fGet.avgP50 << " ns | "
                      << std::setw(8) << fGet.avgP99 << " ns"
                      << std::endl;
            std::cout << std::left << std::setw(20) << "Baseline (no NUMA)" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << std::setw(14) << bSet.avgOpsPerSec << " | "
                      << std::setw(8) << bSet.avgP50 << " ns | "
                      << std::setw(14) << bGet.avgOpsPerSec << " | "
                      << std::setw(8) << bGet.avgP50 << " ns | "
                      << std::setw(8) << bGet.avgP99 << " ns"
                      << std::endl;

            double setDelta = (bSet.avgP50 > 0) ? ((bSet.avgP50 - fSet.avgP50) / bSet.avgP50 * 100.0) : 0;
            double getDelta = (bGet.avgP50 > 0) ? ((bGet.avgP50 - fGet.avgP50) / bGet.avgP50 * 100.0) : 0;
            std::cout << std::endl;
            std::cout << "Furrballs Set delta: " << std::fixed << std::setprecision(1) << setDelta << "% vs baseline" << std::endl;
            std::cout << "Furrballs Get delta: " << std::fixed << std::setprecision(1) << getDelta << "% vs baseline" << std::endl;
        }

        {
            std::cout << std::endl;
            std::cout << "Concurrent throughput (4 threads, 2500 keys/thread, 64B):" << std::endl;

            double blSetOpsSum = 0, blGetOpsSum = 0;
            double fSetOpsSum = 0, fGetOpsSum = 0;

            for (int i = 0; i < iterations; i++) {
                auto fSet = benchMultiThread(fb_fresh, "Set", 4, numNodes, 2500, 64, true, false);
                auto fGet = benchMultiThread(fb_fresh, "Get", 4, numNodes, 2500, 64, false, false);
                fSetOpsSum += fSet.opsPerSec;
                fGetOpsSum += fGet.opsPerSec;

                {
                    std::vector<ThreadArg> args(4);
                    std::vector<std::thread> threads(4);
                    std::vector<std::vector<ns>> setLat(4);
                    for (int t = 0; t < 4; t++) {
                        args[t].fb = nullptr;
                        args[t].startKey = t * 2500;
                        args[t].endKey = args[t].startKey + 2500;
                        args[t].valueSize = 64;
                        args[t].doSet = true;
                        args[t].latencies = &setLat[t];
                        args[t].pinNode = t % numNodes;
                    }
                    auto t0 = Clock::now();
                    for (int t = 0; t < 4; t++) {
                        threads[t] = std::thread([&baseline, &args, t]() {
                            if (args[t].pinNode >= 0) Numatic::PinCurrentThreadToNode(args[t].pinNode);
                            std::vector<char> val(64, 'X');
                            size_t n = args[t].endKey - args[t].startKey;
                            args[t].latencies->resize(n);
                            for (size_t i = 0; i < n; i++) {
                                baseline.Set("bl_" + std::to_string(args[t].startKey + i), val.data(), 64);
                            }
                        });
                    }
                    for (auto& th : threads) th.join();
                    auto t1 = Clock::now();
                    blSetOpsSum += (4 * 2500) / std::chrono::duration<double, std::milli>(t1 - t0).count() * 1000.0;
                }

                {
                    auto t0 = Clock::now();
                    std::vector<std::thread> threads(4);
                    for (int t = 0; t < 4; t++) {
                        threads[t] = std::thread([&baseline, t, numNodes]() {
                            Numatic::PinCurrentThreadToNode(t % numNodes);
                            std::vector<char> outBuf(128);
                            for (size_t i = 0; i < 2500; i++) {
                                size_t outSize = 0;
                                auto t0 = Clock::now();
                                baseline.Get("bl_" + std::to_string(t * 2500 + i), outBuf.data(), outBuf.size(), outSize);
                                volatile auto t1 = Clock::now();
                                (void)t1;
                            }
                        });
                    }
                    for (auto& th : threads) th.join();
                    auto t1 = Clock::now();
                    blGetOpsSum += (4 * 2500) / std::chrono::duration<double, std::milli>(t1 - t0).count() * 1000.0;
                }
            }

            std::cout << std::left << std::setw(20) << "Implementation" << " | "
                      << std::right << std::setw(14) << "Set ops/s" << " | "
                      << std::setw(14) << "Get ops/s"
                      << std::endl;
            std::cout << std::string(60, '-') << std::endl;
            std::cout << std::left << std::setw(20) << "Furrballs" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << std::setw(14) << fSetOpsSum / iterations << " | "
                      << std::setw(14) << fGetOpsSum / iterations
                      << std::endl;
            std::cout << std::left << std::setw(20) << "Baseline (no NUMA)" << " | "
                      << std::right << std::fixed << std::setprecision(0)
                      << std::setw(14) << blSetOpsSum / iterations << " | "
                      << std::setw(14) << blGetOpsSum / iterations
                      << std::endl;

            double setSpeedup = (blSetOpsSum > 0) ? ((fSetOpsSum / iterations) / (blSetOpsSum / iterations)) : 0;
            double getSpeedup = (blGetOpsSum > 0) ? ((fGetOpsSum / iterations) / (blGetOpsSum / iterations)) : 0;
            std::cout << std::endl;
            std::cout << "Furrballs concurrent Set speedup: " << std::fixed << std::setprecision(2) << setSpeedup << "x vs baseline" << std::endl;
            std::cout << "Furrballs concurrent Get speedup: " << std::fixed << std::setprecision(2) << getSpeedup << "x vs baseline" << std::endl;
        }

        if (fb_fresh != fb) delete fb_fresh;
    }

    // --- ZIPFIAN WORKLOAD ---
    if (numNodes >= 2) {
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

        auto benchZipfian = [&](FurrBall* fball, const std::string& name, int nNodes) -> void {
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
                        std::string key = "zipf_t" + std::to_string(nodeId) + "_" + std::to_string(i);
                        fball->Set(key, value.data(), 64);
                    }

                    int otherNode = (nodeId + 1) % nNodes;

                    tr.selfGetLatencies.resize(zipfReadOps);
                    tr.crossGetLatencies.resize(zipfReadOps);
                    tr.localHits = 0;
                    tr.totalGets = zipfReadOps * 2;

                    for (size_t i = 0; i < zipfReadOps; i++) {
                        size_t selfIdx = zipfianSample(zipfKeysPerThread, 0.99, rng);
                        std::string selfKey = "zipf_t" + std::to_string(nodeId) + "_" + std::to_string(selfIdx);
                        size_t outSize = 0;
                        auto t0 = Clock::now();
                        fball->Get(selfKey, outBuf.data(), outBuf.size(), outSize);
                        tr.selfGetLatencies[i] = Clock::now() - t0;
                    }

                    for (size_t i = 0; i < zipfReadOps; i++) {
                        size_t crossIdx = zipfianSample(zipfKeysPerThread, 0.99, rng);
                        std::string crossKey = "zipf_t" + std::to_string(otherNode) + "_" + std::to_string(crossIdx);
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

        NumaConfig numaTL;
        numaTL.AllocateUsingNodePageSize = false;
        numaTL.UseThreadLocalRouting = true;

        FurrConfig configTL;
        configTL.EnableLogging = false;
        configTL.EnableNUMA = true;
        configTL.PageSize = 4096;
        configTL.InitialPageCount = 2048;
        configTL.numaConfig = &numaTL;

        FurrBall* fb_tl = FurrBall::CreateBall("BenchDB_ZF", configTL);
        if (fb_tl) {
            benchZipfian(fb, "Round-robin", numNodes);
            benchZipfian(fb_tl, "Thread-local", numNodes);
            delete fb_tl;
        } else {
            std::cout << "Failed to create thread-local FurrBall for Zipfian test" << std::endl;
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
                    // Single-threaded Set
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
                    // Single-threaded Get
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

                // Multi-threaded self/cross Get
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

                // Counters
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

            FurrBall* fb_sm = FurrBall::CreateBall("BenchDB_SM_FB", cfgSM);
            if (fb_sm) {
                benchVariant(*fb_sm, "Furrballs (shared+SeqLock)", "smfb", false);
                delete fb_sm;
            }
            benchVariant(*sm, "SharedNothing (MPSC queue)", "smsm", true);

            delete sm;
        }
    }

    std::cout << std::endl;
    std::cout << "Cache Stats:" << std::endl;
    std::cout << "  HitCount: " << fb->Stats.GetHitCount() << std::endl;
    std::cout << "  MissCount: " << fb->Stats.GetMissCount() << std::endl;
    std::cout << "  BytesWritten: " << fb->Stats.GetBytesWritten() << std::endl;
    std::cout << "  BytesRead: " << fb->Stats.GetBytesRead() << std::endl;

    delete fb;
    FurrBall::Shutdown();
    return 0;
}
