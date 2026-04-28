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
#include <functional>
#include <random>
#include <cmath>

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;

struct BenchStats {
    double p50, p99, avg, opsPerSec;
};

template<typename Policy>
struct PolicyBenchResult {
    BenchStats setStats, getStats;
    double scanNsPerPass = 0;
    double scanMHz = 0;
};

template<typename Policy>
PolicyBenchResult<Policy> benchPolicy(const std::string& label, size_t numOps, int iters, size_t valueSize, int scanIters) {
    using FB = FurrBall<Policy>;

    NumaConfig numaConfig;
    numaConfig.AllocateUsingNodePageSize = false;

    FurrConfig config;
    config.EnableLogging = false;
    config.EnableNUMA = true;
    config.PageSize = 4096;
    config.InitialPageCount = 2048;
    config.numaConfig = &numaConfig;

    std::string dbPath = "BenchDB_" + label;
    FB* fb = FB::CreateBall(dbPath, config);
    if (!fb) {
        std::cerr << "Cannot create FurrBall<" << label << ">\n";
        return {};
    }

    std::vector<char> value(valueSize, 'X');
    std::vector<char> outBuf(valueSize + 64);
    std::string prefix = label + "_key_";

    PolicyBenchResult<Policy> result;
    BenchStats& aggSet = result.setStats;
    BenchStats& aggGet = result.getStats;

    for (int i = 0; i < iters; i++) {
        std::vector<ns> lats(numOps);
        for (size_t j = 0; j < numOps; j++) {
            std::string key = prefix + std::to_string(j);
            auto t0 = Clock::now();
            fb->Set(key, value.data(), valueSize);
            lats[j] = Clock::now() - t0;
        }
        std::sort(lats.begin(), lats.end());
        double total = 0;
        for (auto& l : lats) total += l.count();
        double p50 = lats[numOps/2].count();
        double p99 = lats[numOps*99/100].count();
        double ops = (total > 0) ? (numOps / (total / 1e9)) : 0;
        aggSet.p50 += p50; aggSet.p99 += p99; aggSet.avg += total / numOps; aggSet.opsPerSec += ops;
        std::cout << "  iter " << i << " Set: "
                  << std::fixed << std::setprecision(0)
                  << std::setw(10) << ops << " ops/s | "
                  << "p50 " << std::setw(8) << p50 << " ns | "
                  << "p99 " << std::setw(8) << p99 << " ns\n";
    }
    aggSet.p50 /= iters; aggSet.p99 /= iters; aggSet.avg /= iters; aggSet.opsPerSec /= iters;

    for (int i = 0; i < iters; i++) {
        std::vector<ns> lats(numOps);
        for (size_t j = 0; j < numOps; j++) {
            std::string key = prefix + std::to_string(j);
            size_t outSize = 0;
            auto t0 = Clock::now();
            fb->Get(key, outBuf.data(), outBuf.size(), outSize);
            lats[j] = Clock::now() - t0;
        }
        std::sort(lats.begin(), lats.end());
        double total = 0;
        for (auto& l : lats) total += l.count();
        double p50 = lats[numOps/2].count();
        double p99 = lats[numOps*99/100].count();
        double ops = (total > 0) ? (numOps / (total / 1e9)) : 0;
        aggGet.p50 += p50; aggGet.p99 += p99; aggGet.avg += total / numOps; aggGet.opsPerSec += ops;
        std::cout << "  iter " << i << " Get: "
                  << std::fixed << std::setprecision(0)
                  << std::setw(10) << ops << " ops/s | "
                  << "p50 " << std::setw(8) << p50 << " ns | "
                  << "p99 " << std::setw(8) << p99 << " ns\n";
    }
    aggGet.p50 /= iters; aggGet.p99 /= iters; aggGet.avg /= iters; aggGet.opsPerSec /= iters;

    if constexpr (Policy::HasScanner) {
        std::cout << "  SIMD Scan: ";
        double totalScanNs = 0;
        for (int i = 0; i < scanIters; i++) {
            auto t0 = Clock::now();
            fb->ScanAndExecute(0);
            totalScanNs += (Clock::now() - t0).count();
        }
        result.scanNsPerPass = totalScanNs / scanIters;
        size_t totalKeys = 0;
        for (int n = 0; n < Numatic::GetNodeCount(); n++) {
            (void)n;
            totalKeys += numOps;
        }
        result.scanMHz = (totalScanNs > 0) ? (totalKeys / (totalScanNs / scanIters / 1e9)) / 1e6 : 0;
        std::cout << std::fixed << std::setprecision(0)
                  << result.scanNsPerPass << " ns/pass | "
                  << std::setprecision(1) << result.scanMHz << " Mkeys/s\n";
    } else {
        std::cout << "  SIMD Scan: N/A (HasScanner=false)\n";
    }

    delete fb;
    return result;
}

void printRow(const std::string& label, const BenchStats& s, const BenchStats& g, double scanNs, double scanMHz) {
    std::cout << std::left << std::setw(20) << label
              << "Set " << std::setw(8) << std::fixed << std::setprecision(0) << s.opsPerSec << "/s"
              << " p50 " << std::setw(6) << s.p50
              << " | Get " << std::setw(8) << g.opsPerSec << "/s"
              << " p50 " << std::setw(6) << g.p50
              << " | Scan " << std::setw(7) << std::setprecision(0) << scanNs << " ns"
              << " " << std::setprecision(1) << scanMHz << " Mkeys/s\n";
}

static size_t zipfianSample(size_t n, double theta, std::mt19937_64& rng) {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double u = dist(rng);
    double zeta = 0.0;
    for (size_t i = 1; i <= n; i++) zeta += 1.0 / std::pow((double)i, theta);
    double val = u * zeta;
    size_t k = 1;
    while (k < n) {
        double sum = 1.0 / std::pow((double)k, theta);
        if (val <= sum) break;
        val -= sum;
        k++;
    }
    return k - 1;
}

int main() {
    FurrBall<StandardRemarc>::Bootstrap();
    int numNodes = Numatic::GetNodeCount();
    size_t numOps = 10000;
    int iters = 5;
    size_t valSize = 64;
    int scanIters = 100;

    std::cout << "=== INTEGRATED POLICY BENCHMARK ===" << std::endl;
    std::cout << "NUMA nodes: " << numNodes << " | " << iters << " iters | "
              << numOps << " ops | " << valSize << " B | "
              << scanIters << " scan iters\n\n";

    std::cout << "--- ArcPolicy (plain ARC) ---" << std::endl;
    auto arc = benchPolicy<ArcPolicy>("arc", numOps, iters, valSize, scanIters);

    std::cout << "\n--- StandardRemarc (REMARC scoring + SIMD scan) ---" << std::endl;
    auto remarc = benchPolicy<StandardRemarc>("remarc", numOps, iters, valSize, scanIters);

    std::cout << "\n--- AugAdaptPolicy (adaptive heap + SIMD scan) ---" << std::endl;
    auto aug = benchPolicy<AugAdaptPolicy>("aug", numOps, iters, valSize, scanIters);

    std::cout << "\n--- NativeRemarcPolicy (desire-only, no ARC) ---" << std::endl;
    auto native = benchPolicy<NativeRemarcPolicy>("native", numOps, iters, valSize, scanIters);

    std::cout << "\n=== SUMMARY ===" << std::endl;
    printRow("ArcPolicy", arc.setStats, arc.getStats, arc.scanNsPerPass, arc.scanMHz);
    printRow("StandardRemarc", remarc.setStats, remarc.getStats, remarc.scanNsPerPass, remarc.scanMHz);
    printRow("AugAdaptPolicy", aug.setStats, aug.getStats, aug.scanNsPerPass, aug.scanMHz);
    printRow("NativeRemarc", native.setStats, native.getStats, native.scanNsPerPass, native.scanMHz);

    std::cout << "\n  REMARC vs ARC Set throughput: "
              << std::setprecision(1)
              << ((remarc.setStats.opsPerSec - arc.setStats.opsPerSec) / arc.setStats.opsPerSec * 100.0) << "%\n";
    std::cout << "  AUG-ADAPT vs ARC Set throughput: "
              << ((aug.setStats.opsPerSec - arc.setStats.opsPerSec) / arc.setStats.opsPerSec * 100.0) << "%\n";
    std::cout << "  NATIVE-REMARc vs ARC Set throughput: "
              << ((native.setStats.opsPerSec - arc.setStats.opsPerSec) / arc.setStats.opsPerSec * 100.0) << "%\n";
    std::cout << "  REMARC vs ARC Get p50: "
              << std::setprecision(1)
              << ((remarc.getStats.p50 - arc.getStats.p50) / arc.getStats.p50 * 100.0) << "%\n";
    std::cout << "  AUG-ADAPT vs ARC Get p50: "
              << ((aug.getStats.p50 - arc.getStats.p50) / arc.getStats.p50 * 100.0) << "%\n";
    std::cout << "  NATIVE-REMARc vs ARC Get p50: "
              << ((native.getStats.p50 - arc.getStats.p50) / arc.getStats.p50 * 100.0) << "%\n";

    std::cout << "\n=== HIT-RATE PRESSURE TEST ===" << std::endl;
    std::cout << "Oversubscribed cache, Zipf 0.99 workload, 4 policies\n\n";

    {
        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;

        const size_t numUniverse = 20000;
        const size_t accessOps = 100000;
        const double zipfTheta = 0.99;
        const size_t valSize = 64;
        const int pageCount = 128;

        auto runHitRate = [&](const std::string& label, auto makeBall) -> double {
            FurrConfig fc;
            fc.EnableLogging = false;
            fc.EnableNUMA = true;
            fc.PageSize = 4096;
            fc.InitialPageCount = pageCount;
            fc.numaConfig = &nc;

            auto* fb = makeBall(fc);
            if (!fb) { std::cerr << "Cannot create " << label << "\n"; return 0; }

            std::vector<char> val(valSize, 'X');
            std::vector<char> outBuf(valSize + 64);
            std::mt19937_64 rng(12345);

            size_t hits = 0, misses = 0;

            for (size_t i = 0; i < accessOps; i++) {
                size_t keyIdx = zipfianSample(numUniverse, zipfTheta, rng);
                std::string key = "hr_" + std::to_string(keyIdx);
                size_t outSize = 0;
                Error err = fb->Get(key, outBuf.data(), outBuf.size(), outSize);
                if (err == NO_ERR) {
                    hits++;
                } else {
                    misses++;
                    fb->Set(key, val.data(), valSize);
                }
                if constexpr (requires { fb->ManagePages(0, false); }) {
                    if ((i + 1) % 5000 == 0) fb->ManagePages(0, false);
                }
            }

            double hitRate = 100.0 * hits / accessOps;
            std::cout << std::left << std::setw(20) << label
                      << "hits " << std::setw(6) << hits
                      << " misses " << std::setw(6) << misses
                      << " hit rate " << std::fixed << std::setprecision(1)
                      << hitRate << "%\n";

            delete fb;
            return hitRate;
        };

        auto arcHR = runHitRate("ArcPolicy", [](FurrConfig& fc) {
            return FurrBall<ArcPolicy>::CreateBall("hr_arc", fc);
        });
        auto remarcHR = runHitRate("StandardRemarc", [](FurrConfig& fc) {
            return FurrBall<StandardRemarc>::CreateBall("hr_remarc", fc);
        });
        auto augHR = runHitRate("AugAdaptPolicy", [](FurrConfig& fc) {
            return FurrBall<AugAdaptPolicy>::CreateBall("hr_aug", fc);
        });
        auto nativeHR = runHitRate("NativeRemarc", [](FurrConfig& fc) {
            return FurrBall<NativeRemarcPolicy>::CreateBall("hr_native", fc);
        });

        std::cout << "\n  ARC hit rate: " << std::fixed << std::setprecision(1) << arcHR << "%\n";
        std::cout << "  REMARC vs ARC: " << std::setprecision(1) << (remarcHR - arcHR) << " pp\n";
        std::cout << "  AUG vs ARC: " << (augHR - arcHR) << " pp\n";
        std::cout << "  NATIVE vs ARC: " << (nativeHR - arcHR) << " pp\n";
    }

    std::cout << "\n=== SCAN RESISTANCE TEST ===" << std::endl;
    std::cout << "Zipf warmup -> full sequential scan -> Zipf recovery, 2 policies\n\n";

    {
        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;

        const size_t hotUniverse = 5000;
        const size_t scanStart = 5000;
        const size_t scanRange = 30000;
        const size_t valSize = 64;
        const int pageCount = 64;

        auto runScan = [&](const std::string& label, auto makeBall) -> double {
            FurrConfig fc;
            fc.EnableLogging = false;
            fc.EnableNUMA = true;
            fc.PageSize = 4096;
            fc.InitialPageCount = pageCount;
            fc.numaConfig = &nc;

            auto* fb = makeBall(fc);
            if (!fb) { std::cerr << "Cannot create " << label << "\n"; return 0; }

            std::vector<char> val(valSize, 'X');
            std::vector<char> outBuf(valSize + 64);
            std::mt19937_64 rng(99999);

            std::cout << "  [" << label << "]\n";

            size_t warmupHits = 0;
            for (size_t i = 0; i < 30000; i++) {
                size_t keyIdx = zipfianSample(hotUniverse, 0.99, rng);
                std::string key = "scan_" + std::to_string(keyIdx);
                size_t outSize = 0;
                if (fb->Get(key, outBuf.data(), outBuf.size(), outSize) != NO_ERR)
                    fb->Set(key, val.data(), valSize);
                else
                    warmupHits++;
            }
            double warmupHR = 100.0 * warmupHits / 30000;
            std::cout << "    Warmup (Zipf 0.99, 30k): " << std::setprecision(1) << warmupHR << "%\n";

            size_t scanHits = 0;
            for (size_t i = 0; i < scanRange; i++) {
                std::string key = "scan_" + std::to_string(scanStart + i);
                size_t outSize = 0;
                if (fb->Get(key, outBuf.data(), outBuf.size(), outSize) == NO_ERR)
                    scanHits++;
                else
                    fb->Set(key, val.data(), valSize);
                if constexpr (requires { fb->ManagePages(0, false); }) {
                    if ((i + 1) % 5000 == 0) fb->ManagePages(0, false);
                }
            }
            double scanHR = 100.0 * scanHits / scanRange;
            std::cout << "    Scan (seq " << scanRange << " cold keys): " << scanHR << "%\n";

            size_t recoveryHits = 0;
            for (size_t i = 0; i < 30000; i++) {
                size_t keyIdx = zipfianSample(hotUniverse, 0.99, rng);
                std::string key = "scan_" + std::to_string(keyIdx);
                size_t outSize = 0;
                if (fb->Get(key, outBuf.data(), outBuf.size(), outSize) == NO_ERR)
                    recoveryHits++;
                else
                    fb->Set(key, val.data(), valSize);
                if constexpr (requires { fb->ManagePages(0, false); }) {
                    if ((i + 1) % 5000 == 0) fb->ManagePages(0, false);
                }
            }
            double recoveryHR = 100.0 * recoveryHits / 30000;
            std::cout << "    Recovery (Zipf 0.99, 30k): " << recoveryHR << "%\n";

            delete fb;
            return recoveryHR;
        };

        auto arcRec = runScan("ArcPolicy", [](FurrConfig& fc) {
            return FurrBall<ArcPolicy>::CreateBall("scan_arc", fc);
        });
        auto nativeRec = runScan("NativeRemarc", [](FurrConfig& fc) {
            return FurrBall<NativeRemarcPolicy>::CreateBall("scan_native", fc);
        });

        std::cout << "\n  ARC recovery: " << std::fixed << std::setprecision(1) << arcRec << "%\n";
        std::cout << "  NATIVE recovery: " << nativeRec << "%\n";
        std::cout << "  NATIVE vs ARC recovery: " << std::setprecision(1) << (nativeRec - arcRec) << " pp\n";
    }

    std::cout << "\n=== PAGE MANAGEMENT BENCHMARK ===" << std::endl;
    std::cout << "Random-order insert, hard oversubscription, A/B: migration vs eviction-only\n\n";

    {
        using FB = FurrBall<StandardRemarc>;
        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        FurrConfig fc;
        fc.EnableLogging = false;
        fc.EnableNUMA = true;
        fc.PageSize = 4096;
        fc.InitialPageCount = 512;
        fc.remarcConfig.ThetaEvict = 6;
        fc.numaConfig = &nc;

        auto runHardWorkload = [&](bool disableMigration) {
            FurrConfig localFc = fc;
            localFc.DisableMigration = disableMigration;

            std::mt19937_64 rng(42);
            std::vector<char> val(64, 'X');
            std::vector<char> outBuf(128);

            FB* fb = FB::CreateBall("BenchDB_page", localFc);
            if (!fb) { std::cerr << "Cannot create page bench FurrBall\n"; return; }

            const size_t totalKeys = 30000;
            const size_t hotKeys = 3000;
            const size_t numPhases = 3;

            std::cout << "  Phase 1: Loading " << totalKeys << " keys in RANDOM order..." << std::flush;
            std::vector<size_t> loadOrder(totalKeys);
            std::iota(loadOrder.begin(), loadOrder.end(), 0);
            std::shuffle(loadOrder.begin(), loadOrder.end(), rng);
            for (size_t idx : loadOrder) {
                fb->Set("pg_key_" + std::to_string(idx), val.data(), 64);
            }
            std::cout << " done\n";

            std::cout << "  Phase 2: " << numPhases << " phases (Zipf 0.99, 30k ops + ManagePages each)...\n";
            size_t totalMigrated = 0, totalEvicted = 0, totalPages = 0;
            double totalScanUs = 0;
            for (size_t phase = 0; phase < numPhases; phase++) {
                for (size_t i = 0; i < 30000; i++) {
                    size_t keyIdx = zipfianSample(totalKeys, 0.99, rng);
                    size_t outSize = 0;
                    fb->Get("pg_key_" + std::to_string(keyIdx), outBuf.data(), outBuf.size(), outSize);
                }
                auto mr = fb->ManagePages(0, true);
                totalMigrated += mr.keysMigrated;
                totalPages += mr.pagesEvicted;
                totalEvicted += mr.keysEvicted;
                totalScanUs += mr.scanNs / 1000.0;
                std::cout << "    Phase " << phase << ": " << mr.keysMigrated << " migrated, "
                          << mr.pagesEvicted << " pg evicted (" << mr.scanNs / 1000 << " us)\n";
            }

            std::cout << "  Phase 3: Oversubscribing +15k keys..." << std::flush;
            for (size_t i = totalKeys; i < totalKeys + 15000; i++) {
                fb->Set("pg_key_" + std::to_string(i), val.data(), 64);
            }
            std::cout << " done\n";

            std::cout << "  Phase 4: Final ManagePages..." << std::flush;
            auto finalMr = fb->ManagePages(0, true);
            totalMigrated += finalMr.keysMigrated;
            totalPages += finalMr.pagesEvicted;
            totalEvicted += finalMr.keysEvicted;
            totalScanUs += finalMr.scanNs / 1000.0;
            std::cout << " " << finalMr.keysMigrated << " migrated, "
                      << finalMr.pagesEvicted << " pg evicted\n";

            size_t hotHits = 0;
            for (size_t i = 0; i < hotKeys; i++) {
                size_t outSize = 0;
                if (fb->Get("pg_key_" + std::to_string(i), outBuf.data(), outBuf.size(), outSize) == NO_ERR) hotHits++;
            }
            size_t midHits = 0;
            for (size_t i = hotKeys; i < hotKeys + 5000; i++) {
                size_t outSize = 0;
                if (fb->Get("pg_key_" + std::to_string(i), outBuf.data(), outBuf.size(), outSize) == NO_ERR) midHits++;
            }
            size_t coldHits = 0;
            for (size_t i = 15000; i < 20000; i++) {
                size_t outSize = 0;
                if (fb->Get("pg_key_" + std::to_string(i), outBuf.data(), outBuf.size(), outSize) == NO_ERR) coldHits++;
            }
            size_t tailHits = 0;
            for (size_t i = 25000; i < totalKeys; i++) {
                size_t outSize = 0;
                if (fb->Get("pg_key_" + std::to_string(i), outBuf.data(), outBuf.size(), outSize) == NO_ERR) tailHits++;
            }

            std::cout << "  Results (total: " << totalMigrated << " migrated, "
                      << totalPages << " pg, " << totalEvicted << " keys evicted"
                      << ", avg scan " << std::setprecision(0) << totalScanUs / (numPhases + 1) << " us):\n"
                      << "    Hot   (0-2.9k):  " << std::setw(5) << hotHits << "/" << hotKeys
                      << "  (" << std::fixed << std::setprecision(1)
                      << (100.0 * hotHits / hotKeys) << "%)\n"
                      << "    Mid   (3k-7.9k): " << std::setw(5) << midHits << "/5000"
                      << "  (" << (100.0 * midHits / 5000) << "%)\n"
                      << "    Cold  (15k-20k): " << std::setw(5) << coldHits << "/5000"
                      << "  (" << (100.0 * coldHits / 5000) << "%)\n"
                      << "    Tail  (25k-30k): " << std::setw(5) << tailHits << "/5000"
                      << "  (" << (100.0 * tailHits / 5000) << "%)\n\n";

            delete fb;
        };

        std::cout << "====== WITH MIGRATION (desire + bit-vector swap) ======\n";
        runHardWorkload(false);

        std::cout << "====== WITHOUT MIGRATION (eviction-only, same budget) ======\n";
        runHardWorkload(true);
    }

    FurrBall<StandardRemarc>::Shutdown();
    return 0;
}
