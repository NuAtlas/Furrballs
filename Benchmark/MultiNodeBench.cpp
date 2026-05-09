#include "Furrballs.h"
#include "Numatic.h"
#include "NodeJob.h"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/cache.h>
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <iomanip>
#include <random>
#include <filesystem>
#include <thread>
#include <atomic>
#include <cmath>

using namespace NuAtlas;
using Clock = std::chrono::high_resolution_clock;
using us = std::chrono::microseconds;

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

static rocksdb::DB* openRocksDB(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.optimize_filters_for_hits = true;
    rocksdb::BlockBasedTableOptions tableOpts;
    tableOpts.block_cache = rocksdb::NewLRUCache(0);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));
    rocksdb::DB* db = nullptr;
    auto s = rocksdb::DB::Open(options, path, &db);
    if (!s.ok()) { std::cerr << "RocksDB open failed: " << s.ToString() << "\n"; return nullptr; }
    return db;
}

static void SetupSimulated2Nodes() {
    auto& gs = Detail::globalNumaState;
    if (gs.Workers) {
        for (int i = 0; i < gs.NumaNodeCount; i++)
            gs.Workers[i].~NodeJob();
        free(gs.Workers);
        gs.Workers = nullptr;
    }
    gs.NumaNodeCount = 2;
    gs.SysNumaPageSize = 65536;
    gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * 2);
    for (int i = 0; i < 2; i++) {
        new(&gs.Workers[i]) NodeJob(i);
        gs.Workers[i].Start([](){});
    }
    gs.Initialized = true;
}

int main() {
    std::cout << std::fixed << std::setprecision(1);
    SetupSimulated2Nodes();

    const size_t valSize = 64;
    const size_t pagesPerNode = 16;
    const double zipfTheta = 0.99;

    std::cout << "=== 2-NODE POLICY BENCHMARK (no page pressure) ===\n";
    std::cout << pagesPerNode << " pages/node (" << (pagesPerNode * 4096 / 1024) << "KB each), "
              << "Zipf theta=" << zipfTheta << "\n\n";

    auto run = [&](const std::string& label, bool isRemarc,
                    size_t localKeys, size_t sharedKeys,
                    size_t warmupOps, size_t measureOps) {

        size_t totalKeys = localKeys * 2 + sharedKeys;
        std::string dbPath = "/tmp/policybench_" + label;
        std::filesystem::remove_all(dbPath);
        std::string fbPath = "/tmp/policybench_fb_" + label;
        std::filesystem::remove_all(fbPath);

        rocksdb::DB* rdb = openRocksDB(dbPath);
        if (!rdb) return;

        FurrConfig fc;
        fc.EnableLogging = false;
        fc.EnableNUMA = true;
        fc.IsVolatile = true;
        fc.PageSize = 4096;
        fc.InitialPageCount = pagesPerNode;
        NumaConfig nc;
        nc.AllocateUsingNodePageSize = false;
        nc.UseThreadLocalRouting = true;
        fc.numaConfig = &nc;
        fc.DisableMigration = false;
        fc.remarcConfig.MaxDeadAge = 8;
        fc.remarcConfig.StaticEvictThresh = -1.0f;
        fc.remarcConfig.ThetaEvict = 30;

        FurrBall<StandardRemarc>* fbR = nullptr;
        FurrBall<ArcPolicy>* fbA = nullptr;
        if (isRemarc)
            fbR = FurrBall<StandardRemarc>::CreateBall(fbPath, fc);
        else
            fbA = FurrBall<ArcPolicy>::CreateBall(fbPath, fc);

        if ((!fbR && isRemarc) || (!fbA && !isRemarc)) {
            std::cerr << "  FAILED: " << label << "\n";
            delete rdb;
            std::filesystem::remove_all(dbPath);
            return;
        }

        std::vector<char> val(valSize, 'X');
        std::vector<char> outBuf(valSize + 64);

        auto doSet = [&](int node, const std::string& key) -> bool {
            Error err = OUT_OF_MEM;
            if (isRemarc) err = fbR->Set(key, val.data(), valSize);
            else err = fbA->Set(key, val.data(), valSize);
            return err == NO_ERR;
        };

        auto doGet = [&](int node, const std::string& key, char* buf, size_t sz, size_t& outSz) -> bool {
            Error err = INVALID_ARG;
            if (isRemarc) err = fbR->Get(key, buf, sz, outSz);
            else err = fbA->Get(key, buf, sz, outSz);
            return err == NO_ERR;
        };

        auto genKey = [&](int node, size_t ki) -> std::string {
            if (ki < localKeys)
                return "n" + std::to_string(node) + "_k" + std::to_string(ki);
            else
                return "sh_k" + std::to_string(ki - localKeys);
        };

        size_t setSize = 0, setFail = 0;

        for (int node = 0; node < 2; node++) {
            Numatic::SetCurrentNodeOverride(node);
            for (size_t i = 0; i < localKeys; i++) {
                if (doSet(node, "n" + std::to_string(node) + "_k" + std::to_string(i)))
                    setSize++;
                else
                    setFail++;
            }
        }
        for (size_t i = 0; i < sharedKeys; i++) {
            if (doSet(0, "sh_k" + std::to_string(i)))
                setSize++;
            else
                setFail++;
        }

        std::cout << "  [" << label << " populate: " << setSize << " ok, " << setFail << " fail]\n";
        if (setFail > 0) {
            std::cout << "  WARNING: page pressure detected, results unreliable\n";
        }

        auto nodeWorker = [&](int nodeId) {
            Numatic::SetCurrentNodeOverride(nodeId);
            std::mt19937_64 rng(42 + nodeId * 9999);
            size_t keySpace = localKeys + sharedKeys;
            std::atomic<size_t>& hitCounter = *(new std::atomic<size_t>(0));
            std::atomic<size_t>& missCounter = *(new std::atomic<size_t>(0));
            std::atomic<size_t>& sharedHitCounter = *(new std::atomic<size_t>(0));
            std::atomic<size_t>& sharedMissCounter = *(new std::atomic<size_t>(0));
            std::atomic<size_t>& remoteHitCounter = *(new std::atomic<size_t>(0));

            for (size_t i = 0; i < measureOps; i++) {
                size_t ki = zipfianSample(keySpace, zipfTheta, rng);
                std::string key = genKey(nodeId, ki);
                bool isShared = (ki >= localKeys);

                size_t outSize = 0;
                if (doGet(nodeId, key, outBuf.data(), outBuf.size(), outSize)) {
                    hitCounter.fetch_add(1);
                    if (isShared) sharedHitCounter.fetch_add(1);
                } else {
                    missCounter.fetch_add(1);
                    if (isShared) sharedMissCounter.fetch_add(1);
                }
            }

            struct { std::atomic<size_t>* p; const char* n; } counters[] = {
                {&hitCounter, "hits"}, {&missCounter, "miss"},
                {&sharedHitCounter, "sh_hits"}, {&sharedMissCounter, "sh_miss"},
                {&remoteHitCounter, "remote_hits"}
            };
            for (auto& c : counters)
                std::cout << "    node" << nodeId << " " << c.n << "=" << c.p->load();
            std::cout << "\n";
            for (auto& c : counters)
                delete c.p;
        };

        auto t0 = Clock::now();
        std::thread th0(nodeWorker, 0);
        std::thread th1(nodeWorker, 1);
        th0.join();
        th1.join();
        auto t1 = Clock::now();

        size_t totalOps = measureOps * 2;
        int64_t elapsed = std::chrono::duration_cast<us>(t1 - t0).count();
        double ms = elapsed / 1000.0;

        size_t totalHits = 0, totalMiss = 0;
        if (isRemarc) {
            totalHits = fbR->Stats.GetHitCount();
            totalMiss = fbR->Stats.GetMissCount();
        } else {
            totalHits = fbA->Stats.GetHitCount();
            totalMiss = fbA->Stats.GetMissCount();
        }

        double hitRate = totalHits + totalMiss > 0
            ? 100.0 * totalHits / (totalHits + totalMiss) : 0;

        std::cout << "    total: HR=" << hitRate << "%"
                  << " throughput=" << (totalOps / (elapsed / 1e6)) << " Mops"
                  << " " << ms << "ms";

        if (isRemarc && fbR) {
            std::cout << "\n    REMARC: migrate=" << fbR->Stats.GetMigrationCount()
                      << " local-hit=" << fbR->Stats.GetLocalHitCount()
                      << " cold-rot=" << fbR->Stats.GetColdPageRotations()
                      << " set-fail(populate)=" << setFail;
        }
        if (!isRemarc && fbA) {
            std::cout << "\n    ARC: local-hit=" << fbA->Stats.GetLocalHitCount()
                      << " cold-rot=" << fbA->Stats.GetColdPageRotations();
        }
        std::cout << "\n\n";

        Numatic::ClearCurrentNodeOverride();
        delete fbR;
        delete fbA;
        delete rdb;
        std::filesystem::remove_all(dbPath);
        std::filesystem::remove_all(fbPath);
    };

    std::cout << "--- Scenario 1: 200 local + 100 shared keys ---\n";
    run("arc_s1", false, 200, 100, 0, 20000);
    run("rm_s1", true,  200, 100, 0, 20000);

    std::cout << "--- Scenario 2: 200 local + 400 shared keys ---\n";
    run("arc_s2", false, 200, 400, 0, 20000);
    run("rm_s2", true,  200, 400, 0, 20000);

    std::cout << "--- Scenario 3: 100 local + 600 shared keys (shared-heavy) ---\n";
    run("arc_s3", false, 100, 600, 0, 20000);
    run("rm_s3", true,  100, 600, 0, 20000);

    FurrBall<StandardRemarc>::Shutdown();
    return 0;
}
