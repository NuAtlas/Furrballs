#pragma once

// Trace replay for cache policies: trace loading, synthetic workloads, exact
// reference policies (LRU, FIFO, Belady OPT) and a get-or-set replay driver.
// Miss ratio is a property of the policy, not the hardware, so everything here
// is meaningful on a single-socket laptop.

#include "CMap.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <list>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace TraceSim {

    // A trace is a sequence of object ids. Object sizes are ignored: the
    // Furrballs policies count objects, not bytes.
    using Trace = std::vector<uint64_t>;

    // ---------------------------------------------------------------- loading

    // libCacheSim "oracleGeneral" binary format: packed 24-byte records
    // {u32 time, u64 obj_id, u32 obj_size, i64 next_access_vtime}. This is the
    // format of the public datasets used by the S3-FIFO paper (decompress the
    // .zst files first).
    inline Trace LoadOracleGeneral(const std::string& path, size_t maxRequests = SIZE_MAX) {
        std::ifstream in(path, std::ios::binary);
        if (!in) throw std::runtime_error("cannot open " + path);
        Trace trace;
        constexpr size_t kRecord = 24;
        std::vector<char> buf(kRecord * 65536);
        while (trace.size() < maxRequests && in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            size_t records = static_cast<size_t>(in.gcount()) / kRecord;
            for (size_t r = 0; r < records && trace.size() < maxRequests; r++) {
                uint64_t id;
                std::memcpy(&id, buf.data() + r * kRecord + 4, sizeof(id));
                trace.push_back(id);
            }
        }
        return trace;
    }

    // Text traces: one request per line. With keyColumn >= 0 the line is split
    // on commas and that column is the key (e.g. column 1 of the Twitter
    // twemcache CSVs). Keys are hashed to 64-bit ids.
    inline Trace LoadText(const std::string& path, int keyColumn = -1, bool skipHeader = false,
                          size_t maxRequests = SIZE_MAX) {
        std::ifstream in(path);
        if (!in) throw std::runtime_error("cannot open " + path);
        Trace trace;
        std::string line;
        if (skipHeader) std::getline(in, line);
        while (trace.size() < maxRequests && std::getline(in, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            std::string_view key = line;
            if (keyColumn >= 0) {
                size_t start = 0;
                for (int c = 0; c < keyColumn && start != std::string_view::npos; c++) {
                    start = key.find(',', start);
                    if (start != std::string_view::npos) start++;
                }
                if (start == std::string_view::npos) continue;
                size_t end = key.find(',', start);
                key = key.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
            }
            if (key.empty()) continue;
            trace.push_back(XXH3_64bits(key.data(), key.size()));
        }
        return trace;
    }

    // -------------------------------------------------------------- synthetic

    // Scatters ranks over the id space so popularity is not correlated with
    // key order (and so with hash-table position).
    inline uint64_t RankToId(uint64_t rank) { return XXH3_64bits(&rank, sizeof(rank)); }

    class ZipfSampler {
        std::vector<double> cdf_;

    public:
        ZipfSampler(size_t numKeys, double theta) : cdf_(numKeys) {
            double sum = 0;
            for (size_t i = 0; i < numKeys; i++) cdf_[i] = (sum += 1.0 / std::pow(double(i + 1), theta));
            for (double& c : cdf_) c /= sum;
        }
        template <class Rng> uint64_t operator()(Rng& rng) {
            double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
            return static_cast<uint64_t>(std::lower_bound(cdf_.begin(), cdf_.end(), u) - cdf_.begin());
        }
    };

    inline Trace Zipf(size_t numKeys, size_t numRequests, double theta, uint64_t seed = 1) {
        ZipfSampler zipf(numKeys, theta);
        std::mt19937_64 rng(seed);
        Trace trace(numRequests);
        for (auto& id : trace) id = RankToId(zipf(rng));
        return trace;
    }

    // Zipf traffic interrupted by sequential scans over never-repeated keys.
    // scanFraction of all requests belong to scans of scanLength keys each.
    inline Trace ZipfWithScans(size_t numKeys, size_t numRequests, double theta, double scanFraction,
                               size_t scanLength, uint64_t seed = 1) {
        ZipfSampler zipf(numKeys, theta);
        std::mt19937_64 rng(seed);
        std::bernoulli_distribution startScan(scanFraction / double(scanLength) /
                                              std::max(1e-9, 1.0 - scanFraction));
        Trace trace;
        trace.reserve(numRequests);
        uint64_t nextScanKey = uint64_t(1) << 62;  // disjoint from zipf ranks
        while (trace.size() < numRequests) {
            if (startScan(rng)) {
                for (size_t i = 0; i < scanLength && trace.size() < numRequests; i++)
                    trace.push_back(RankToId(nextScanKey++));
            } else {
                trace.push_back(RankToId(zipf(rng)));
            }
        }
        return trace;
    }

    inline size_t CountUnique(const Trace& trace) {
        Trace sorted = trace;
        std::sort(sorted.begin(), sorted.end());
        return static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());
    }

    // ------------------------------------------------------ reference policies
    // Exact, single-threaded textbook implementations, used as baselines and to
    // check that the concurrent policies behave like the algorithms they name.

    class RefLRU {
        size_t cap_;
        std::list<uint64_t> order_;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> where_;

    public:
        explicit RefLRU(size_t cap) : cap_(cap) { where_.reserve(cap * 2); }
        bool Access(uint64_t id) {
            auto it = where_.find(id);
            if (it != where_.end()) {
                order_.splice(order_.begin(), order_, it->second);
                return true;
            }
            if (order_.size() >= cap_) {
                where_.erase(order_.back());
                order_.pop_back();
            }
            order_.push_front(id);
            where_[id] = order_.begin();
            return false;
        }
    };

    class RefFIFO {
        size_t cap_;
        std::list<uint64_t> order_;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> where_;

    public:
        explicit RefFIFO(size_t cap) : cap_(cap) { where_.reserve(cap * 2); }
        bool Access(uint64_t id) {
            if (where_.count(id)) return true;
            if (order_.size() >= cap_) {
                where_.erase(order_.back());
                order_.pop_back();
            }
            order_.push_front(id);
            where_[id] = order_.begin();
            return false;
        }
    };

    // Belady's OPT with bypass: on a miss, evict whichever of the cached
    // objects or the new one is next used furthest in the future. A lower
    // bound on the miss ratio of any (demand-paging) policy.
    inline size_t BeladyMisses(const Trace& trace, size_t cap) {
        const size_t n = trace.size();
        constexpr size_t kNever = SIZE_MAX;
        std::vector<size_t> nextUse(n);
        {
            std::unordered_map<uint64_t, size_t> last;
            last.reserve(n / 4);
            for (size_t i = n; i-- > 0;) {
                auto it = last.find(trace[i]);
                nextUse[i] = it == last.end() ? kNever : it->second;
                last[trace[i]] = i;
            }
        }
        std::unordered_map<uint64_t, size_t> cached;  // id -> next use
        cached.reserve(cap * 2);
        std::set<std::pair<size_t, uint64_t>> byNext;  // (next use, id)
        size_t misses = 0;
        for (size_t i = 0; i < n; i++) {
            uint64_t id = trace[i];
            auto it = cached.find(id);
            if (it != cached.end()) {
                byNext.erase({it->second, id});
                it->second = nextUse[i];
                byNext.insert({nextUse[i], id});
                continue;
            }
            misses++;
            if (cap == 0 || nextUse[i] == kNever) continue;  // bypass
            if (cached.size() >= cap) {
                auto furthest = std::prev(byNext.end());
                if (furthest->first <= nextUse[i]) continue;  // bypass
                cached.erase(furthest->second);
                byNext.erase(furthest);
            }
            cached[id] = nextUse[i];
            byNext.insert({nextUse[i], id});
        }
        return misses;
    }

    // ------------------------------------------------------------------ replay

    // Shape of a Furrballs page handle; only the policies' bookkeeping matters.
    struct ReplayValue {
        void* DataOffset;
        uint32_t DataSize;
        uint64_t id;
    };

    struct ReplayResult {
        size_t requests = 0;
        size_t misses = 0;
        size_t failedSets = 0;  // Set returned an error (counted as misses)
        double seconds = 0;
        double MissRatio() const { return requests ? double(misses) / double(requests) : 0; }
        double MopsPerSec() const { return seconds > 0 ? double(requests) / seconds / 1e6 : 0; }
    };

    inline std::string IdKey(uint64_t id) { return std::string(reinterpret_cast<const char*>(&id), sizeof(id)); }

    // Get-or-set replay: Find, and Set on a miss. With threads > 1, requests are
    // dealt out in chunks of 64 so each thread sees roughly the global order;
    // the reported miss ratio is then that of the interleaving that happened.
    template <class Store>
    ReplayResult ReplayConcurrent(const Trace& trace, size_t cap, unsigned threads = 1) {
        Store store(cap);
        constexpr size_t kChunk = 64;
        std::atomic<size_t> misses{0}, failed{0};
        auto worker = [&](unsigned t) {
            size_t localMisses = 0, localFailed = 0;
            for (size_t base = size_t(t) * kChunk; base < trace.size(); base += size_t(threads) * kChunk) {
                size_t end = std::min(trace.size(), base + kChunk);
                for (size_t i = base; i < end; i++) {
                    std::string key = IdKey(trace[i]);
                    if (store.Find(key)) continue;
                    localMisses++;
                    ReplayValue v{&store, 1, trace[i]};
                    if (store.Set(key, v) != NuAtlas::NO_ERR) localFailed++;
                }
            }
            misses += localMisses;
            failed += localFailed;
        };
        auto t0 = std::chrono::steady_clock::now();
        if (threads <= 1) {
            worker(0);
        } else {
            std::vector<std::thread> ts;
            for (unsigned t = 0; t < threads; t++) ts.emplace_back(worker, t);
            for (auto& th : ts) th.join();
        }
        ReplayResult r;
        r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        r.requests = trace.size();
        r.misses = misses;
        r.failedSets = failed;
        return r;
    }

    template <class Ref>
    ReplayResult ReplayReference(const Trace& trace, size_t cap) {
        Ref ref(cap);
        ReplayResult r;
        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t id : trace) r.misses += !ref.Access(id);
        r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        r.requests = trace.size();
        return r;
    }

    inline ReplayResult ReplayBelady(const Trace& trace, size_t cap) {
        ReplayResult r;
        auto t0 = std::chrono::steady_clock::now();
        r.misses = BeladyMisses(trace, cap);
        r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        r.requests = trace.size();
        return r;
    }

    // Replay the way FurrBall drives a store: its key capacity is `slack`
    // times what fits (FurrBall sizes it as bytes / sizeof(KeyMeta)), and
    // eviction comes from memory pressure through ForceEvictOne() once `cap`
    // keys are resident. Single-threaded.
    template <class Store>
    ReplayResult ReplayForced(const Trace& trace, size_t cap, size_t slack) {
        Store store(cap * slack);
        size_t resident = 0;
        store.SetEvictionCallback([&](const ReplayValue&) { resident--; });
        ReplayResult r;
        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t id : trace) {
            std::string key = IdKey(id);
            if (store.Find(key)) continue;
            r.misses++;
            while (resident >= cap && store.ForceEvictOne()) {}
            if (store.Set(key, ReplayValue{&store, 1, id}) == NuAtlas::NO_ERR) resident++;
            else r.failedSets++;
        }
        r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        r.requests = trace.size();
        return r;
    }

    // ReplayForced for the concurrent policies, by Replay() name.
    inline ReplayResult ReplayForced(const std::string& policy, const Trace& trace, size_t cap, size_t slack) {
        using namespace NuAtlas;
        if (policy == "lru") return ReplayForced<ConcurrentLRU<ReplayValue>>(trace, cap, slack);
        if (policy == "arc") return ReplayForced<ConcurrentARC<ReplayValue>>(trace, cap, slack);
        if (policy == "s3fifo") return ReplayForced<ConcurrentS3FIFO<ReplayValue>>(trace, cap, slack);
        if (policy == "sharded-lru")
            return ReplayForced<ShardedStore<ConcurrentLRU<ReplayValue>>>(trace, cap, slack);
        if (policy == "sharded-arc")
            return ReplayForced<ShardedStore<ConcurrentARC<ReplayValue>>>(trace, cap, slack);
        if (policy == "sharded-s3fifo")
            return ReplayForced<ShardedStore<ConcurrentS3FIFO<ReplayValue>>>(trace, cap, slack);
        throw std::invalid_argument("not a concurrent policy: " + policy);
    }

    // Policy names accepted by Replay().
    inline const std::vector<std::string>& PolicyNames() {
        static const std::vector<std::string> names = {"lru",         "arc",            "s3fifo",
                                                       "sharded-lru", "sharded-arc",    "sharded-s3fifo",
                                                       "ref-lru",     "ref-fifo",       "opt"};
        return names;
    }

    inline bool IsConcurrentPolicy(const std::string& policy) {
        return policy == "lru" || policy == "arc" || policy == "s3fifo" || policy.starts_with("sharded-");
    }

    inline ReplayResult Replay(const std::string& policy, const Trace& trace, size_t cap, unsigned threads = 1) {
        using namespace NuAtlas;
        if (policy == "lru") return ReplayConcurrent<ConcurrentLRU<ReplayValue>>(trace, cap, threads);
        if (policy == "arc") return ReplayConcurrent<ConcurrentARC<ReplayValue>>(trace, cap, threads);
        if (policy == "s3fifo") return ReplayConcurrent<ConcurrentS3FIFO<ReplayValue>>(trace, cap, threads);
        if (policy == "sharded-lru")
            return ReplayConcurrent<ShardedStore<ConcurrentLRU<ReplayValue>>>(trace, cap, threads);
        if (policy == "sharded-arc")
            return ReplayConcurrent<ShardedStore<ConcurrentARC<ReplayValue>>>(trace, cap, threads);
        if (policy == "sharded-s3fifo")
            return ReplayConcurrent<ShardedStore<ConcurrentS3FIFO<ReplayValue>>>(trace, cap, threads);
        if (policy == "ref-lru") return ReplayReference<RefLRU>(trace, cap);
        if (policy == "ref-fifo") return ReplayReference<RefFIFO>(trace, cap);
        if (policy == "opt") return ReplayBelady(trace, cap);
        throw std::invalid_argument("unknown policy: " + policy);
    }

} // namespace TraceSim
