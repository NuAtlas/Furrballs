#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "Furrballs.h"
#include "Numatic.h"
#include "NodeJob.h"

using namespace NuAtlas;

static void percentiles(std::vector<int64_t>& lat, const char* label) {
    if (lat.empty()) { printf("  %s: no data\n", label); return; }
    std::sort(lat.begin(), lat.end());
    size_t n = lat.size();
    auto p = [&](double f) -> int64_t {
        size_t idx = std::min((size_t)(f * (n - 1)), n - 1);
        return lat[idx];
    };
    printf("  %-12s  n=%zu  p50=%ld  p90=%ld  p99=%ld  p99.9=%ld  max=%ld ns\n",
           label, n, p(0.50), p(0.90), p(0.99), p(0.999), lat.back());
}

template<typename Policy>
static int runBench(int pagesPerNode, int valueSize, int universe,
                    int opsPerThread, int threads, float writeRatio,
                    int reserveCap) {
    printf("=== EvictBench <%s> ===\n", typeid(Policy).name());
    printf("  pagesPerNode=%d  valueSize=%d  universe=%d  ops=%d  threads=%d  writeRatio=%.0f%%  reserve=%d\n",
           pagesPerNode, valueSize, universe, opsPerThread, threads, writeRatio * 100, reserveCap);
    fflush(stdout);

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

    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    nc.UseThreadLocalRouting = false;

    FurrConfig cfg;
    cfg.InitialPageCount = pagesPerNode;
    cfg.PageSize = 65536;
    cfg.IsVolatile = true;
    cfg.EnableNUMA = true;
    cfg.EnableLogging = false;
    cfg.ReserveCapacity = reserveCap;
    cfg.numaConfig = &nc;

    char dbPath[256];
    snprintf(dbPath, sizeof(dbPath), "/tmp/evictbench_%d", (int)getpid());

    auto* fb = FurrBall<Policy>::CreateBall(dbPath, cfg, true);
    if (!fb) {
        fprintf(stderr, "CreateBall failed\n");
        return 1;
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    std::vector<std::vector<int64_t>> getLats(threads);
    std::vector<std::vector<int64_t>> setLats(threads);
    std::vector<std::atomic<size_t>> hits(threads);
    std::vector<std::atomic<size_t>> misses(threads);
    for (int i = 0; i < threads; i++) { hits[i] = 0; misses[i] = 0; }

    auto worker = [&](int tid) {
        std::mt19937_64 rng(tid * 12345 + 67890);
        std::uniform_int_distribution<uint64_t> keyDist(0, universe - 1);
        std::uniform_real_distribution<float> opDist(0.0f, 1.0f);

        std::vector<uint8_t> buf(valueSize, 0);
        for (int i = 0; i < valueSize; i++) buf[i] = (uint8_t)(i + tid);

        auto& gl = getLats[tid];
        auto& sl = setLats[tid];
        gl.reserve(opsPerThread);
        sl.reserve(opsPerThread);

        for (int op = 0; op < opsPerThread; op++) {
            uint64_t k = keyDist(rng);
            std::string key = std::to_string(k);

            if (opDist(rng) < writeRatio) {
                auto start = std::chrono::high_resolution_clock::now();
                fb->Set(key, buf.data(), buf.size());
                auto end = std::chrono::high_resolution_clock::now();
                sl.push_back((end - start).count());
            } else {
                std::vector<uint8_t> outBuf(valueSize);
                size_t outSize = 0;
                auto start = std::chrono::high_resolution_clock::now();
                Error err = fb->Get(key, outBuf.data(), outBuf.size(), outSize);
                auto end = std::chrono::high_resolution_clock::now();
                gl.push_back((end - start).count());
                if (err == NO_ERR) hits[tid]++;
                else misses[tid]++;
            }
        }
    };

    std::vector<std::thread> thrs;
    for (int i = 0; i < threads; i++) thrs.emplace_back(worker, i);
    for (auto& t : thrs) t.join();

    auto t1 = std::chrono::high_resolution_clock::now();
    double elapsedS = std::chrono::duration<double>(t1 - t0).count();

    size_t totalHits = 0, totalMisses = 0;
    for (int i = 0; i < threads; i++) { totalHits += hits[i]; totalMisses += misses[i]; }
    double hitRate = (totalHits + totalMisses > 0) ? 100.0 * totalHits / (totalHits + totalMisses) : 0;
    size_t totalOps = (size_t)threads * opsPerThread;

    printf("  elapsed=%.2fs  ops=%.0fK/s  hitRate=%.1f%%\n",
           elapsedS, totalOps / elapsedS / 1000.0, hitRate);

    std::vector<int64_t> allGet, allSet;
    for (int i = 0; i < threads; i++) {
        allGet.insert(allGet.end(), getLats[i].begin(), getLats[i].end());
        allSet.insert(allSet.end(), setLats[i].begin(), setLats[i].end());
    }
    percentiles(allGet, "GET");
    percentiles(allSet, "SET");

    delete fb;
    gs.Workers[0].Stop();

    printf("\n");
    return 0;
}

int main(int argc, char** argv) {
    int pagesPerNode = argc > 1 ? atoi(argv[1]) : 64;
    int valueSize = argc > 2 ? atoi(argv[2]) : 64;
    int universe = argc > 3 ? atoi(argv[3]) : 50000;
    int opsPerThread = argc > 4 ? atoi(argv[4]) : 200000;
    int threads = argc > 5 ? atoi(argv[5]) : 1;
    float writeRatio = argc > 6 ? atof(argv[6]) : 0.30f;
    int reserveCap = argc > 7 ? atoi(argv[7]) : 2;
    const char* policy = argc > 8 ? argv[8] : "arc";

    if (strcmp(policy, "lru") == 0) {
        return runBench<LruPolicy>(pagesPerNode, valueSize, universe,
                                   opsPerThread, threads, writeRatio, reserveCap);
    } else {
        return runBench<ArcPolicy>(pagesPerNode, valueSize, universe,
                                   opsPerThread, threads, writeRatio, reserveCap);
    }
}
