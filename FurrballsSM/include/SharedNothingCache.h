#pragma once
#include "Error.h"
#include "Page.h"
#include "Numatic.h"
#include "SeqLock.h"
#include <string>
#include <atomic>
#include <vector>
#include <memory>
#include <shared_mutex>

namespace NuAtlas::SM {

struct SMConfig {
    size_t PageSize = 4096;
    size_t InitialPageCount = 2048;
    bool IsVolatile = false;
};

class SharedNothingCache {
public:
    struct KeyMeta {
        size_t PageIndex;
        size_t DataSize;
        void* DataOffset;
        int NodeID;
    };

    class Statistics {
        friend SharedNothingCache;
    private:
        alignas(64) std::atomic<size_t> TotalAllocated{0};
        alignas(64) std::atomic<unsigned int> HitCount{0};
        alignas(64) std::atomic<unsigned int> MissCount{0};
        alignas(64) std::atomic<size_t> BytesWritten{0};
        alignas(64) std::atomic<size_t> BytesRead{0};
        alignas(64) std::atomic<unsigned int> LocalHitCount{0};
        Statistics() noexcept = default;
        Statistics& operator=(const Statistics&) = delete;
        Statistics& operator=(Statistics&&) = delete;
    public:
        unsigned int GetHitCount() const noexcept { return HitCount.load(); }
        unsigned int GetMissCount() const noexcept { return MissCount.load(); }
        size_t GetBytesWritten() const noexcept { return BytesWritten.load(); }
        size_t GetBytesRead() const noexcept { return BytesRead.load(); }
        unsigned int GetLocalHitCount() const noexcept { return LocalHitCount.load(); }
    } Stats;

    static void Bootstrap();
    static void Shutdown();

    static SharedNothingCache* Create(const std::string& dbPath, const SMConfig& config = SMConfig()) noexcept;

    Error Set(const std::string& key, void* data, size_t size) noexcept;
    Error Get(const std::string& key, void* outBuf, size_t bufSize, size_t& outSize) noexcept;

    ~SharedNothingCache() noexcept;

private:
    struct Impl;
    Impl* pImpl;

    SharedNothingCache(const SMConfig& config, size_t numPages) noexcept;
    SharedNothingCache(const SharedNothingCache&) = delete;
};

}
