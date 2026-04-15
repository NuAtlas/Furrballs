/*****************************************************************//**
 * \file   Furrballs.h
 * \brief Primary interface for the Furrball library.
 *
 * NUMA-aware caching and database management using RocksDB with ARC eviction.
 *
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#pragma once
#include "Error.h"
#include "Page.h"
#include "Logger.h"
#include "NodeJob.h"
#include "Numatic.h"
#include "WaitGroup.h"
#include "SeqLock.h"
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <shared_mutex>
#include <list>
#include <type_traits>
#include <mutex>
#include <optional>
#include <algorithm>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#undef max
#undef min
#else
#include <unistd.h>
#endif
#ifdef _DEBUG
#define DEBUG
#endif

namespace NuAtlas {
    template<class Key, class Value>
    class Cache {
    protected:
        virtual void evict() = 0;
    public:
        using EvictionCallback = std::function<void(const Key&, Value&)>;
        virtual bool Contains(const Key& key)const noexcept = 0;
        virtual void Touch(const Key& key)noexcept = 0;
        virtual void Add(const Key& key, const Value& value) = 0;
        virtual Value Get(const Key& key) = 0;
        virtual void Set(const Key& key, const Value& value) = 0;
        virtual void SetEvictionCallback(EvictionCallback cb) = 0;
    };

    template<class Key, class Value>
    class ARCPolicy final : public Cache<Key, Value> {
    public:
        using typename Cache<Key, Value>::EvictionCallback;
    private:
        std::list<Key> t1;
        std::list<Key> t2;
        std::list<Key> b1;
        std::list<Key> b2;
        std::unordered_map<Key, Value> map;
        size_t capacity;
        size_t p;
        EvictionCallback evictionCallback = [](const Key&, Value&) {};

        void replace(const Key& key) {
            if (!t1.empty() && (t1.size() > p || (std::find(b2.begin(), b2.end(), key) != b2.end() && t1.size() == p))) {
                auto old = t1.back();
                t1.pop_back();
                b1.push_front(old);
                map.erase(old);
            }
            else {
                auto old = t2.back();
                t2.pop_back();
                b2.push_front(old);
                map.erase(old);
            }
        }

        void evict() override {
            if (t1.size() + b1.size() >= capacity) {
                if (t1.size() < capacity) {
                    b1.pop_back();
                }
                else {
                    auto key = t1.back();
                    evictionCallback(key, map[key]);
                    t1.pop_back();
                }
            }
            if (t1.size() + t2.size() + b1.size() + b2.size() >= 2 * capacity) {
                if (t2.size() + b2.size() > capacity) {
                    b2.pop_back();
                }
                else {
                    auto key = t2.back();
                    evictionCallback(key, map[key]);
                    t2.pop_back();
                }
            }
        }

    public:
        ARCPolicy(size_t cap) : capacity(cap), p(1) {}

        void SetEvictionCallback(EvictionCallback cb) override {
            evictionCallback = cb;
        };

        bool Contains(const Key& key)const noexcept override {
            return map.find(key) != map.end();
        }

        void Touch(const Key& key)noexcept override {
            if (std::find(t1.begin(), t1.end(), key) != t1.end()) {
                t1.remove(key);
                t2.push_front(key);
            }
            else if (std::find(t2.begin(), t2.end(), key) != t2.end()) {
                t2.splice(t2.begin(), t2, std::find(t2.begin(), t2.end(), key));
            }
            else if (std::find(b1.begin(), b1.end(), key) != b1.end()) {
                size_t delta1 = b1.size() > 0 ? b2.size() / b1.size() : 1;
                p = std::min(capacity, p + std::max(delta1, (size_t)1));
                replace(key);
                b1.remove(key);
                t2.push_front(key);
                map[key] = Value();
            }
            else if (std::find(b2.begin(), b2.end(), key) != b2.end()) {
                size_t delta2 = b2.size() > 0 ? b1.size() / b2.size() : 1;
                p = std::max((size_t)0, static_cast<size_t>(p) - std::max(delta2, (size_t)1));
                replace(key);
                b2.remove(key);
                t2.push_front(key);
                map[key] = Value();
            }
        }

        void Add(const Key& key, const Value& value) override {
            if (map.size() >= capacity) {
                evict();
            }
            t1.push_front(key);
            map[key] = value;
        }

        Value Get(const Key& key) override {
            Touch(key);
            return map[key];
        }

        void Set(const Key& key, const Value& value) override {
            if (Contains(key)) {
                map[key] = value;
                Touch(key);
            }
            else {
                Add(key, value);
            }
        }

        void ForEachValue(std::function<void(const Key&, Value&)> fn) {
            for (auto& kv : map) {
                fn(kv.first, kv.second);
            }
        }

        void ForEachValue(std::function<void(const Key&, const Value&)> fn) const {
            for (const auto& kv : map) {
                fn(kv.first, kv.second);
            }
        }
    };

    template<bool use_atomic = true>
    struct RoundRobin{
private:
        int n = 1;
        std::conditional_t<use_atomic, std::atomic_uint, unsigned int> counter = 0;
    public:
        RoundRobin() = default;
        RoundRobin(int _n) : n(_n) {};
        ~RoundRobin() = default;

        void SetN(int _n) { n = _n; }
        int Get(int step = 1){
            if constexpr (use_atomic){
                return counter.fetch_add(step, std::memory_order_relaxed) % n;
            }else{

                return counter++ % n;
            }
        }
    };
    typedef RoundRobin<true> AtomicRoundRobin;
    //All default values are arbitrary for now.

    struct NumaConfig{
        short PerNodePageLimit = 2;
        bool AllocateUsingNodePageSize = true; //< allocates with NUMA page sizes (or multiples of page sizes) instead of PageSize.
        bool AllowNodeFallback = false; //< For now, This will be ignored (considered false)
        bool UseThreadLocalRouting = false;
        //Add priority later, Add Home Node + Perferred Nodes.
    };

    struct FurrConfig final {
        size_t CapacityLimit = 1024 * 1024;
        size_t InitialPageCount = 2; //< This is a hint, more (logcial) pages may be allocated. 
        size_t PageSize = 4096; //< This is used for Logical division of the Page, The actually page may be larger (NUMA case, AMP may also expand a page, etc...)
        Cache<size_t, void*>::EvictionCallback evictionCallback = [](const size_t&, void*&) {};

        union {
            struct {
                bool IsVolatile : 1;
                bool EnableLogging : 1;
                bool EnableNUMA : 1;
            };
            uint8_t flags = 0;
        };

        NumaConfig* numaConfig = nullptr;
    };

    class FurrBall final {
    private:
        struct ImplDetail;
        ImplDetail* DataMembers;

        ARCPolicy<size_t, Page*> cache;
        std::shared_mutex rwMutex;
        size_t PageSize;
        size_t MaxPages;
        bool Volatile;
        bool UseNUMA;
        bool ThreadLocalRoute;

        std::vector<void*> AllocatedPages;

        inline static std::list<FurrBall*> OpenBalls = std::list<FurrBall*>();

        struct GlobalNumaState {
            NodeJob* Workers = nullptr;
            size_t SysNumaPageSize = 0;
            int NumaNodeCount = 0;
        };
        static GlobalNumaState globalNumaState;

        FurrBall(const FurrConfig& config, size_t numPages)noexcept;

        void OnEvict(const size_t& key, Page*& page)noexcept;

        size_t PageIndexForAddress(size_t address)const noexcept {
            return address & ~(PageSize - 1);
        }

        Page* AllocatePage(size_t pageIndex)noexcept;
        void FlushPage(Page* page)noexcept;

    public:
        struct KeyMeta{
            size_t PageIndex, DataSize;
            void* DataOffset;
            int NodeID;
        };

        class Statistics {
            friend FurrBall;
        private:
            alignas(64) std::atomic<size_t> UsedMemory{0};
            alignas(64) std::atomic<size_t> TotalAllocated{0};
            alignas(64) std::atomic<unsigned int> EvictionCount{0};
            alignas(64) std::atomic<unsigned int> HitCount{0};
            alignas(64) std::atomic<unsigned int> MissCount{0};
            alignas(64) std::atomic<size_t> BytesWritten{0};
            alignas(64) std::atomic<size_t> BytesRead{0};
            Statistics()noexcept = default;
            Statistics& operator=(const Statistics&) = delete;
            Statistics& operator=(Statistics&&) = delete;
        public:
            size_t GetUsedMemory()const noexcept { return UsedMemory.load(); }
            size_t GetTotalAllocated()const noexcept { return TotalAllocated.load(); }
            unsigned int GetEvictionCount()const noexcept { return EvictionCount.load(); }
            unsigned int GetHitCount()const noexcept { return HitCount.load(); }
            unsigned int GetMissCount()const noexcept { return MissCount.load(); }
            size_t GetBytesWritten()const noexcept { return BytesWritten.load(); }
            size_t GetBytesRead()const noexcept { return BytesRead.load(); }
        } Stats;

        FurrBall(const FurrBall& cpy) = delete;
        static void Bootstrap();
        static void Shutdown();
        static FurrBall* CreateBall(const std::string& DBpath, const FurrConfig& config = FurrConfig(), bool overwrite = false)noexcept;

        void* Get(void* vAddress)noexcept;
        bool Set(void* data, size_t size, size_t vAddress)noexcept;
        //New API-style
        Error Get(const std::string& key, void* outBuf, size_t BufSize, size_t& outSize) noexcept;
        Error Set(const std::string& key, void* data, size_t size)noexcept;

        const Cache<size_t, Page*>& GetBackingCache()const noexcept {
            return cache;
        }

        ~FurrBall()noexcept;
    };
}
