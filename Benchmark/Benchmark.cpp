#include "Furrballs.h"
#include "Numatic.h"
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

int main() {
    FurrBall::Bootstrap();

    int numNodes = Numatic::GetNodeCount();
    int iterations = 5;
    std::cout << "=== Furrballs Benchmark ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << std::endl;
    std::cout << "NUMA page size: " << Numatic::GetNodePageSize() << std::endl;
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
