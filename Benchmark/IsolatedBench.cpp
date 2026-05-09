#include "Furrballs.h"
#include "Numatic.h"
#include "BaselineCache.h"
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

struct LatencyStats {
    std::vector<int64_t> samples;
    void add(int64_t ns) { samples.push_back(ns); }
    double p50() const { if (samples.empty()) return 0; auto s = samples; std::sort(s.begin(), s.end()); return s[s.size()/2]; }
    double p99() const { if (samples.size() < 100) return p50(); auto s = samples; std::sort(s.begin(), s.end()); return s[s.size()*99/100]; }
    double mean() const { if (samples.empty()) return 0; return std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size(); }
    double stddev() const { double m = mean(); double sq = 0; for (auto v : samples) sq += (v-m)*(v-m); return samples.size() > 1 ? std::sqrt(sq/(samples.size()-1)) : 0; }
};

int main() {
    FurrBall<StandardRemarc>::Bootstrap();

    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    const size_t valSize = 64;

    std::cout << std::fixed << std::setprecision(1);

    std::cout << "=== COMPREHENSIVE BENCHMARK ===\n";
    std::cout << "Measures: hit rate, latency (p50/p99/us), throughput (Mops), I/O\n\n";

    auto run = [&](const std::string& label, bool isRemarc,
                    size_t numPages, size_t keysPerPhase, size_t numPhases,
                    size_t opsPerPhase, size_t revisitOps) {

        std::string dbPath = "/tmp/bench_" + label;
        std::filesystem::remove_all(dbPath);
        rocksdb::DB* rdb = openRocksDB(dbPath);
        if (!rdb) return;

        FurrConfig fc;
        fc.EnableLogging = false;
        fc.EnableNUMA = true;
        fc.IsVolatile = false;
        fc.PageSize = 4096;
        fc.InitialPageCount = numPages;
        fc.numaConfig = &nc;
        fc.remarcConfig.MaxDeadAge = 8;
        fc.remarcConfig.StaticEvictThresh = -1.0f;

        FurrBall<StandardRemarc>* fbR = nullptr;
        FurrBall<ArcPolicy>* fbA = nullptr;
        if (isRemarc)
            fbR = FurrBall<StandardRemarc>::CreateBall("_bn_" + label, fc);
        else
            fbA = FurrBall<ArcPolicy>::CreateBall("_bn_" + label, fc);

        if ((!fbR && isRemarc) || (!fbA && !isRemarc)) {
            std::cerr << "  FAILED: " << label << "\n";
            delete rdb; std::filesystem::remove_all(dbPath); return;
        }

        std::vector<char> val(valSize, 'X');
        std::vector<char> outBuf(valSize + 64);
        std::mt19937_64 rng(42);

        auto doGet = [&](const std::string& key, char* buf, size_t& sz) -> bool {
            if (isRemarc) return fbR->Get(key, buf, outBuf.size(), sz) == NO_ERR;
            else return fbA->Get(key, buf, outBuf.size(), sz) == NO_ERR;
        };
        auto doSet = [&](const std::string& key, void* data, size_t sz) -> bool {
            Error err = OUT_OF_MEM;
            if (isRemarc) {
                err = fbR->Set(key, data, sz);
                rdb->Put(rocksdb::WriteOptions(), key,
                         rocksdb::Slice(static_cast<const char*>(data), sz));
            } else {
                err = fbA->Set(key, data, sz);
                rdb->Put(rocksdb::WriteOptions(), key,
                         rocksdb::Slice(static_cast<const char*>(data), sz));
            }
            return err == NO_ERR;
        };

        LatencyStats getHit, getMiss, setOk, setFail;

        auto t0 = Clock::now();
        size_t totalSets = 0, totalSetFails = 0;

        for (size_t p = 0; p < numPhases; p++) {
            size_t base = p * keysPerPhase;
            for (size_t i = 0; i < opsPerPhase; i++) {
                size_t ki = base + zipfianSample(keysPerPhase, 0.99, rng);
                std::string key = "ph_" + std::to_string(ki);
                size_t outSize = 0;
                auto gt = Clock::now();
                if (doGet(key, outBuf.data(), outSize)) {
                    getHit.add(std::chrono::duration_cast<us>(Clock::now() - gt).count());
                } else {
                    auto st = Clock::now();
                    if (doSet(key, val.data(), valSize)) {
                        setOk.add(std::chrono::duration_cast<us>(Clock::now() - st).count());
                        totalSets++;
                    } else {
                        setFail.add(std::chrono::duration_cast<us>(Clock::now() - st).count());
                        totalSetFails++;
                    }
                }
            }
        }

        size_t revHits = 0, revMiss = 0, rdbGets = 0, rdbPuts = 0, setFails2 = 0;
        auto t1 = Clock::now();

        for (size_t i = 0; i < revisitOps; i++) {
            size_t ki = zipfianSample(keysPerPhase, 0.99, rng);
            std::string key = "ph_" + std::to_string(ki);
            size_t outSize = 0;
            size_t mBefore = 0;
            if (isRemarc) mBefore = fbR->Stats.GetMigrationCount();
            auto gt = Clock::now();
            if (doGet(key, outBuf.data(), outSize)) {
                auto lat = std::chrono::duration_cast<us>(Clock::now() - gt).count();
                if (isRemarc && fbR->Stats.GetMigrationCount() > mBefore) {
                    getMiss.add(lat);
                    revHits++;
                } else {
                    getHit.add(lat);
                }
            } else {
                auto lat = std::chrono::duration_cast<us>(Clock::now() - gt).count();
                getMiss.add(lat);
                revMiss++;
                std::string rval;
                auto s = rdb->Get(rocksdb::ReadOptions(), key, &rval);
                rdbGets++;
                if (s.ok() && !rval.empty()) {
                    auto st = Clock::now();
                    std::vector<char> tmp(rval.data(), rval.data() + rval.size());
                    if (doSet(key, tmp.data(), tmp.size())) {
                        rdbPuts++;
                    } else {
                        setFails2++;
                    }
                }
            }
        }
        auto t2 = Clock::now();

        int64_t revUs = std::chrono::duration_cast<us>(t2 - t1).count();
        int64_t totUs = std::chrono::duration_cast<us>(t2 - t0).count();
        double revMs = revUs / 1000.0;
        double totMs = totUs / 1000.0;
        double revMops = revisitOps / (revUs / 1e6);

        size_t totalMisses = revMiss;
        double hitRate = 100.0 * (revisitOps - totalMisses) / revisitOps;

        std::cout << std::left << std::setw(14) << label
                  << " " << numPages << "pg " << keysPerPhase << "k/ph"
                  << " | HR=" << std::setw(5) << hitRate << "%"
                  << " miss=" << std::setw(5) << totalMisses
                  << "\n"
                  << "  get: p50=" << std::setw(6) << getHit.p50()
                  << "us p99=" << std::setw(6) << getHit.p99()
                  << "us (miss p50=" << std::setw(6) << getMiss.p50()
                  << "us p99=" << std::setw(6) << getMiss.p99() << "us)"
                  << "\n"
                  << "  set: ok=" << totalSets << " fail=" << (totalSetFails + setFails2)
                  << " (phases " << totalSetFails << ", revisit " << setFails2 << ")"
                  << " | rdb-get=" << rdbGets << " rdb-put=" << rdbPuts
                  << "\n"
                  << "  throughput: " << std::setprecision(0) << revMops << " Mops"
                  << " | " << std::setprecision(1) << revMs << "ms revisit"
                  << " " << totMs << "ms total";
        if (!isRemarc && fbA) {
            std::cout << "\n  ARC: reclaim=" << fbA->Stats.GetPagesReclaimed()
                      << " cold-rot=" << fbA->Stats.GetColdPageRotations()
                      << " cold-hit=" << fbA->Stats.GetColdPageHits()
                      << " frozen-persist=" << fbA->Stats.GetFrozenPagesPersisted()
                      << " frozen-hit=" << fbA->Stats.GetFrozenPageHits();
        }
        if (isRemarc && fbR) {
            std::cout << "\n  REMARC: reclaim=" << fbR->Stats.GetPagesReclaimed()
                      << " cold-rot=" << fbR->Stats.GetColdPageRotations()
                      << " cold-hit=" << fbR->Stats.GetColdPageHits()
                      << " frozen-persist=" << fbR->Stats.GetFrozenPagesPersisted()
                      << " frozen-hit=" << fbR->Stats.GetFrozenPageHits()
                      << " migrate=" << fbR->Stats.GetMigrationCount();
        }
        std::cout << "\n\n";

        delete fbR;
        delete fbA;
        delete rdb;
        std::filesystem::remove_all(dbPath);
    };

    std::cout << "--- 1. ARC baseline (8 pages, 800 keys/phase, under-subscribed) ---\n";
    run("arc_8pg", false, 8, 800, 6, 15000, 15000);

    std::cout << "\n--- 2. REMARC (8 pages, 800 keys/phase, under-subscribed) ---\n";
    run("rm_8pg", true, 8, 800, 6, 15000, 15000);

    std::cout << "\n--- 3. ARC (4 pages, 800 keys/phase, over-subscribed 2x) ---\n";
    run("arc_4pg", false, 4, 800, 6, 15000, 15000);

    std::cout << "\n--- 4. REMARC (4 pages, 800 keys/phase, over-subscribed 2x) ---\n";
    run("rm_4pg", true, 4, 800, 6, 15000, 15000);

    std::cout << "\n--- 5. ARC (8 pages, 1600 keys/phase, over-subscribed 4x) ---\n";
    run("arc_8pg_1600", false, 8, 1600, 6, 15000, 15000);

    std::cout << "\n--- 6. REMARC (8 pages, 1600 keys/phase, over-subscribed 4x) ---\n";
    run("rm_8pg_1600", true, 8, 1600, 6, 15000, 15000);

    FurrBall<StandardRemarc>::Shutdown();
    return 0;
}
