#include "Furrballs.h"
#include "Numatic.h"
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

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;

using FB = FurrBall<StandardRemarc>;

int main() {
    FB::Bootstrap();
    int numNodes = Numatic::GetNodeCount();

    NumaConfig numaConfig;
    numaConfig.AllocateUsingNodePageSize = false;

    FurrConfig config;
    config.EnableLogging = false;
    config.EnableNUMA = true;
    config.PageSize = 4096;
    config.InitialPageCount = 2048;
    config.numaConfig = &numaConfig;

    FB* fb = FB::CreateBall("BenchDB_ISO", config);
    if (!fb) { std::cerr << "Cannot create FurrBall\n"; FB::Shutdown(); return -1; }

    BaselineCache baseline(4096, 4096);
    size_t numOps = 10000;
    int iters = 5;

    std::cout << "=== ISOLATED BASELINE COMPARISON (fresh FurrBall) ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << " | " << iters << " iterations | " << numOps << " ops" << std::endl;

    auto benchST = [](auto& cache, const std::string& prefix, size_t n, size_t vs, bool doSet) {
        std::vector<char> value(vs, 'X');
        std::vector<char> outBuf(vs + 64);
        std::vector<ns> latencies(n);

        if (!doSet) {
            for (size_t i = 0; i < n; i++)
                cache.Set(prefix + "_key_" + std::to_string(i), value.data(), vs);
        }

        for (size_t i = 0; i < n; i++) {
            std::string key = prefix + "_key_" + std::to_string(i);
            if (doSet) {
                auto t0 = Clock::now();
                cache.Set(key, value.data(), vs);
                latencies[i] = Clock::now() - t0;
            } else {
                size_t outSize = 0;
                auto t0 = Clock::now();
                cache.Get(key, outBuf.data(), outBuf.size(), outSize);
                latencies[i] = Clock::now() - t0;
            }
        }

        std::sort(latencies.begin(), latencies.end());
        double totalNs = 0;
        for (auto& l : latencies) totalNs += l.count();
        double avgNs = totalNs / n;
        double p50 = latencies[n/2].count();
        double p99 = latencies[n*99/100].count();
        double opsPerSec = (totalNs > 0) ? (n / (totalNs / 1e9)) : 0;
        std::cout << (doSet ? "Set" : "Get") << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(10) << opsPerSec << " ops/s | "
                  << "p50 " << std::setw(8) << p50 << " ns | "
                  << "p99 " << std::setw(8) << p99 << " ns"
                  << std::endl;
        return std::make_pair(p50, opsPerSec);
    };

    // Furrballs first
    std::cout << "\n--- Furrballs ---" << std::endl;
    double fSetP50 = 0, fGetP50 = 0;
    for (int i = 0; i < iters; i++) {
        std::cout << "iter " << i << " Set: ";
        auto [p50, ops] = benchST(*fb, "iso_f", numOps, 64, true);
        fSetP50 += p50;
    }
    fSetP50 /= iters;
    for (int i = 0; i < iters; i++) {
        std::cout << "iter " << i << " Get: ";
        auto [p50, ops] = benchST(*fb, "iso_f", numOps, 64, false);
        fGetP50 += p50;
    }
    fGetP50 /= iters;

    // Baseline
    std::cout << "\n--- Baseline ---" << std::endl;
    double bSetP50 = 0, bGetP50 = 0;
    for (int i = 0; i < iters; i++) {
        std::cout << "iter " << i << " Set: ";
        auto [p50, ops] = benchST(baseline, "iso_b", numOps, 64, true);
        bSetP50 += p50;
    }
    bSetP50 /= iters;
    for (int i = 0; i < iters; i++) {
        std::cout << "iter " << i << " Get: ";
        auto [p50, ops] = benchST(baseline, "iso_b", numOps, 64, false);
        bGetP50 += p50;
    }
    bGetP50 /= iters;

    std::cout << "\n--- Summary ---" << std::endl;
    std::cout << "Furrballs  Set p50: " << std::fixed << std::setprecision(0) << fSetP50 << " ns" << std::endl;
    std::cout << "Baseline   Set p50: " << bSetP50 << " ns" << std::endl;
    std::cout << "Set delta: " << std::setprecision(1) << ((bSetP50 - fSetP50) / bSetP50 * 100.0) << "% vs baseline" << std::endl;
    std::cout << "Furrballs  Get p50: " << std::setprecision(0) << fGetP50 << " ns" << std::endl;
    std::cout << "Baseline   Get p50: " << bGetP50 << " ns" << std::endl;
    std::cout << "Get delta: " << std::setprecision(1) << ((bGetP50 - fGetP50) / bGetP50 * 100.0) << "% vs baseline" << std::endl;

    delete fb;
    FB::Shutdown();
    return 0;
}
