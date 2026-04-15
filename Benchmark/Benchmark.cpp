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
    double p99Ns = 0;
};

static void printResult(const BenchResult& r) {
    std::cout << std::left << std::setw(30) << r.name << " | "
              << std::right << std::setw(8) << r.ops << " ops | "
              << std::setw(10) << std::fixed << std::setprecision(1) << r.opsPerSec << " ops/s | "
              << "avg " << std::setw(8) << std::fixed << std::setprecision(0) << r.avgLatencyNs << " ns | "
              << "p50 " << std::setw(8) << std::fixed << std::setprecision(0) << r.p50Ns << " ns | "
              << "p99 " << std::setw(8) << std::fixed << std::setprecision(0) << r.p99Ns << " ns"
              << std::endl;
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

static BenchResult benchSingleThread(FurrBall* fb, const std::string& prefix, size_t numOps, size_t valueSize, bool doSet) {
    BenchResult result;
    result.name = prefix + (doSet ? " Set" : " Get");
    result.ops = numOps;

    std::vector<char> value(valueSize, 'X');
    std::vector<char> outBuf(valueSize + 64);
    std::vector<ns> latencies(numOps);

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
    result.p50Ns = latencies[numOps / 2].count();
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

    if (doSet) {
        for (int t = 0; t < numThreads; t++)
            threads[t] = std::thread(threadWorker, &args[t]);
        for (auto& th : threads) th.join();
    }

    allLatencies.clear();
    allLatencies.resize(numThreads);
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
        result.p50Ns = flat[flat.size() / 2].count();
        result.p99Ns = flat[flat.size() * 99 / 100].count();
    }

    return result;
}

int main() {
    FurrBall::Bootstrap();

    int numNodes = Numatic::GetNodeCount();
    std::cout << "=== Furrballs Benchmark ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << std::endl;
    std::cout << "NUMA page size: " << Numatic::GetNodePageSize() << std::endl;
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

    std::cout << std::string(90, '-') << std::endl;
    std::cout << "SINGLE-THREADED" << std::endl;
    std::cout << std::string(90, '-') << std::endl;

    printResult(benchSingleThread(fb, "64B", 10000, 64, true));
    printResult(benchSingleThread(fb, "64B", 10000, 64, false));
    printResult(benchSingleThread(fb, "512B", 5000, 512, true));
    printResult(benchSingleThread(fb, "512B", 5000, 512, false));
    printResult(benchSingleThread(fb, "4KB", 1000, 4096, true));
    printResult(benchSingleThread(fb, "4KB", 1000, 4096, false));

    std::cout << std::endl;

    if (numNodes >= 2) {
        std::cout << std::string(90, '-') << std::endl;
        std::cout << "MULTI-THREADED NUMA EFFECT (64B, 5000 keys/thread)" << std::endl;
        std::cout << std::string(90, '-') << std::endl;

        auto mtSet = benchMultiThread(fb, "MT Set local", 2, numNodes, 5000, 64, true, false);
        auto mtGet = benchMultiThread(fb, "MT Get local", 2, numNodes, 5000, 64, false, false);
        auto mtGetX = benchMultiThread(fb, "MT Get cross", 2, numNodes, 5000, 64, false, true);

        printResult(mtSet);
        printResult(mtGet);
        printResult(mtGetX);

        if (mtGet.avgLatencyNs > 0 && mtGetX.avgLatencyNs > 0) {
            double overhead = ((mtGetX.avgLatencyNs - mtGet.avgLatencyNs) / mtGet.avgLatencyNs) * 100.0;
            std::cout << "Cross-node overhead: " << std::fixed << std::setprecision(1) << overhead << "%"
                      << " (local=" << mtGet.avgLatencyNs << "ns, cross=" << mtGetX.avgLatencyNs << "ns)" << std::endl;
        }

        std::cout << std::endl;

        std::cout << std::string(90, '-') << std::endl;
        std::cout << "MULTI-THREADED NUMA EFFECT (512B, 2000 keys/thread)" << std::endl;
        std::cout << std::string(90, '-') << std::endl;

        auto mtSet2 = benchMultiThread(fb, "MT Set local", 2, numNodes, 2000, 512, true, false);
        auto mtGet2 = benchMultiThread(fb, "MT Get local", 2, numNodes, 2000, 512, false, false);
        auto mtGetX2 = benchMultiThread(fb, "MT Get cross", 2, numNodes, 2000, 512, false, true);

        printResult(mtSet2);
        printResult(mtGet2);
        printResult(mtGetX2);

        if (mtGet2.avgLatencyNs > 0 && mtGetX2.avgLatencyNs > 0) {
            double overhead2 = ((mtGetX2.avgLatencyNs - mtGet2.avgLatencyNs) / mtGet2.avgLatencyNs) * 100.0;
            std::cout << "Cross-node overhead (512B): " << std::fixed << std::setprecision(1) << overhead2 << "%"
                      << " (local=" << mtGet2.avgLatencyNs << "ns, cross=" << mtGetX2.avgLatencyNs << "ns)" << std::endl;
        }
    } else {
        std::cout << "Skipping NUMA multi-threaded benchmarks (1 node). Run in QEMU VM for NUMA results." << std::endl;
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
