#include "BaselineCache.h"
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
#include <functional>
#include <mutex>

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

static BenchResult benchST(BaselineCache& cache, const std::string& prefix, size_t numOps, size_t valueSize, bool doSet) {
    BenchResult result;
    result.name = prefix + (doSet ? " Set" : " Get");
    result.ops = numOps;

    std::vector<char> value(valueSize, 'X');
    std::vector<char> outBuf(valueSize + 64);
    std::vector<ns> latencies(numOps);

    if (!doSet) {
        for (size_t i = 0; i < numOps; i++)
            cache.Set(prefix + "_key_" + std::to_string(i), value.data(), valueSize);
    }

    auto start = Clock::now();
    for (size_t i = 0; i < numOps; i++) {
        std::string key = prefix + "_key_" + std::to_string(i);
        if (doSet) {
            auto t0 = Clock::now();
            cache.Set(key, value.data(), valueSize);
            latencies[i] = Clock::now() - t0;
        } else {
            size_t outSize = 0;
            auto t0 = Clock::now();
            cache.Get(key, outBuf.data(), outBuf.size(), outSize);
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

struct IterationResult {
    double avgOpsPerSec = 0;
    double avgP50 = 0;
    double avgP99 = 0;
    double avgStddev = 0;
};

static IterationResult runIterations(BaselineCache& cache, const std::function<BenchResult(BaselineCache&)>& benchFn, int iterations) {
    IterationResult ir;
    std::vector<double> opsPerSec(iterations);
    std::vector<double> p50s(iterations);
    std::vector<double> p99s(iterations);
    std::vector<double> stddevs(iterations);

    for (int i = 0; i < iterations; i++) {
        BenchResult r = benchFn(cache);
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
    int iterations = 5;
    std::cout << "=== Baseline Benchmark (non-NUMA VM) ===" << std::endl;
    std::cout << "Iterations per test: " << iterations << std::endl;
    std::cout << std::endl;

    BaselineCache cache(4096, 4096);

    std::cout << std::string(110, '=') << std::endl;
    std::cout << "SINGLE-THREADED (" << iterations << " iterations, averaged)" << std::endl;
    std::cout << std::string(110, '=') << std::endl;

    std::cout << std::string(110, '-') << std::endl;
    std::cout << "64-byte values, 10000 ops" << std::endl;
    std::cout << std::string(110, '-') << std::endl;
    {
        auto setI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "64B", 10000, 64, true); }, iterations);
        auto getI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "64B", 10000, 64, false); }, iterations);
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
        auto setI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "512B", 5000, 512, true); }, iterations);
        auto getI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "512B", 5000, 512, false); }, iterations);
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
        auto setI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "4KB", 1000, 4096, true); }, iterations);
        auto getI = runIterations(cache, [&](BaselineCache& c){ return benchST(c, "4KB", 1000, 4096, false); }, iterations);
        std::cout << "Set  | " << std::fixed << std::setprecision(0)
                  << "avg " << setI.avgOpsPerSec << " ops/s | p50 " << setI.avgP50 << " ns | p99 " << setI.avgP99 << " ns | stddev " << setI.avgStddev << " ns" << std::endl;
        std::cout << "Get  | " << std::fixed << std::setprecision(0)
                  << "avg " << getI.avgOpsPerSec << " ops/s | p50 " << getI.avgP50 << " ns | p99 " << getI.avgP99 << " ns | stddev " << getI.avgStddev << " ns" << std::endl;
    }
    std::cout << std::endl;

    std::cout << std::string(110, '=') << std::endl;
    std::cout << "CONCURRENT THROUGHPUT (4 threads, 2500 keys/thread, 64B)" << std::endl;
    std::cout << std::string(110, '=') << std::endl;
    {
        double setOpsSum = 0, getOpsSum = 0;
        for (int iter = 0; iter < iterations; iter++) {
            BaselineCache iterCache(4096, 4096);

            {
                std::vector<std::thread> threads(4);
                std::vector<std::vector<ns>> latencies(4);
                for (int t = 0; t < 4; t++) {
                    latencies[t].resize(2500);
                }
                auto t0 = Clock::now();
                for (int t = 0; t < 4; t++) {
                    threads[t] = std::thread([&iterCache, &latencies, t]() {
                        std::vector<char> val(64, 'X');
                        for (size_t i = 0; i < 2500; i++) {
                            auto s = Clock::now();
                            iterCache.Set("bl_" + std::to_string(t * 2500 + i), val.data(), 64);
                            latencies[t][i] = Clock::now() - s;
                        }
                    });
                }
                for (auto& th : threads) th.join();
                auto t1 = Clock::now();
                setOpsSum += (4 * 2500) / std::chrono::duration<double, std::milli>(t1 - t0).count() * 1000.0;
            }

            {
                std::vector<std::thread> threads(4);
                std::vector<std::vector<ns>> latencies(4);
                for (int t = 0; t < 4; t++) {
                    latencies[t].resize(2500);
                }
                auto t0 = Clock::now();
                for (int t = 0; t < 4; t++) {
                    threads[t] = std::thread([&iterCache, &latencies, t]() {
                        std::vector<char> outBuf(128);
                        for (size_t i = 0; i < 2500; i++) {
                            size_t outSize = 0;
                            auto s = Clock::now();
                            iterCache.Get("bl_" + std::to_string(t * 2500 + i), outBuf.data(), outBuf.size(), outSize);
                            latencies[t][i] = Clock::now() - s;
                        }
                    });
                }
                for (auto& th : threads) th.join();
                auto t1 = Clock::now();
                getOpsSum += (4 * 2500) / std::chrono::duration<double, std::milli>(t1 - t0).count() * 1000.0;
            }
        }

        std::cout << "Set (4t) | " << std::fixed << std::setprecision(0)
                  << "avg " << setOpsSum / iterations << " ops/s" << std::endl;
        std::cout << "Get (4t) | " << std::fixed << std::setprecision(0)
                  << "avg " << getOpsSum / iterations << " ops/s" << std::endl;
    }

    std::cout << std::endl;
    std::cout << "Done." << std::endl;
    return 0;
}
