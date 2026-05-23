/*********************************************************************\
 * \file   Furrballs.cpp
 *
 * \author The Sphynx
 * \date   July 2024
\*********************************************************************/

#include "Furrballs.h"
#include "CMap.h"
#include "NativeStore.h"
#undef max
#undef min
#include <cstring>
#include <chrono>
#include <rocksdb/db.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/cache.h>
#include "MemoryManager.h"

#ifdef _WIN32
#define unlikely(cond) (cond)
#else
#define unlikely(cond) (__builtin_expect(!!(cond), 0))
#endif

#ifdef SIMULATE_NUMA_LATENCY_NS
#include <atomic>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif

} // namespace NuAtlas

namespace {
    std::atomic<double> tscCyclesPerNs{0.0};

    inline uint64_t rdtsc() {
        unsigned int lo, hi;
        __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
        return ((uint64_t)hi << 32) | lo;
    }

    void calibrateTsc() {
        double expected = tscCyclesPerNs.load(std::memory_order_relaxed);
        if (expected > 0.0) return;
        auto t0 = std::chrono::high_resolution_clock::now();
        uint64_t c0 = rdtsc();
        while (std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - t0).count() < 500) {}
        uint64_t c1 = rdtsc();
        auto t1 = std::chrono::high_resolution_clock::now();
        double elapsedNs = std::chrono::duration<double, std::nano>(t1 - t0).count();
        double calibrated = static_cast<double>(c1 - c0) / elapsedNs;
        tscCyclesPerNs.store(calibrated, std::memory_order_relaxed);
    }

    inline void simulateCrossNodeLatency() {
        calibrateTsc();
        double cpn = tscCyclesPerNs.load(std::memory_order_relaxed);
        uint64_t target = static_cast<uint64_t>(SIMULATE_NUMA_LATENCY_NS * cpn);
        uint64_t start = rdtsc();
        while ((rdtsc() - start) < target) {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#endif
        }
    }
}
#endif

using namespace NuAtlas;

thread_local std::unordered_set<void*> MemoryManager::ThreadBuffers;

namespace NuAtlas {
    namespace Detail {
        GlobalNumaState globalNumaState;
    }
}

struct FlatDesireMap {
    struct Slot { uint64_t h2; uint8_t desire; };
    static constexpr uint64_t EMPTY = ~0ULL;

    Slot* slots_ = nullptr;
    size_t mask_ = 0;

    void init(size_t capacity) {
        size_t sz = 64;
        while (sz < capacity) sz <<= 1;
        mask_ = sz - 1;
        slots_ = new Slot[sz]();
        for (size_t i = 0; i < sz; i++) slots_[i].h2 = EMPTY;
    }

    ~FlatDesireMap() { delete[] slots_; }

    uint8_t get(uint64_t h2) const noexcept {
        if (!slots_) return 0;
        size_t idx = h2 & mask_;
        for (size_t i = 0; i <= mask_; i++) {
            Slot& s = slots_[idx];
            if (s.h2 == h2) return s.desire;
            if (s.h2 == EMPTY) return 0;
            idx = (idx + 1) & mask_;
        }
        return 0;
    }

    void set(uint64_t h2, uint8_t desire) noexcept {
        if (!slots_) return;
        size_t idx = h2 & mask_;
        for (size_t i = 0; i <= mask_; i++) {
            Slot& s = slots_[idx];
            if (s.h2 == h2 || s.h2 == EMPTY) {
                s.h2 = h2;
                s.desire = desire;
                return;
            }
            idx = (idx + 1) & mask_;
        }
    }
};

template<typename Policy>
struct PerNodeDetails{
    Numatic::NumaLocalMemoryResource nodeMR;
    std::pmr::vector<Page> NodePages;
    typename Policy::template Store<KeyMeta> KeyStore;
    FlatDesireMap ShadowDesire;
    SpinLock DesireLock;
    std::atomic<uint8_t> MinDesire{0};
    std::atomic<size_t> CurrentPage{0};
    std::atomic<size_t> StagingPageIdx{SIZE_MAX};
    void* PhysicalPageInNode = nullptr;
    float EvictThresh = 0.5f;
    size_t PrevMigrationCount = 0;
    size_t PrevDeadKeys = 0;
    float AdaptiveStep = 0.05f;
    uint8_t MaxDeadAge = 8;

    // --- Reserve pages for background eviction ---
    static constexpr size_t kDefaultReserveCapacity = 2;
    size_t kReserveCapacity = kDefaultReserveCapacity;
    std::vector<size_t> ReservePages;
    std::atomic<size_t> ReserveCount{0};
    SpinLock ReserveLock;

    bool TryPopReserve(size_t& outPageIdx) {
        std::lock_guard<SpinLock> guard(ReserveLock);
        size_t cnt = ReserveCount.load(std::memory_order_relaxed);
        if (cnt == 0) return false;
        cnt--;
        outPageIdx = ReservePages[cnt];
        ReserveCount.store(cnt, std::memory_order_release);
        return true;
    }

    bool TryPushReserve(size_t pageIdx) {
        std::lock_guard<SpinLock> guard(ReserveLock);
        size_t cnt = ReserveCount.load(std::memory_order_relaxed);
        if (cnt >= kReserveCapacity) return false;
        ReservePages[cnt] = pageIdx;
        ReserveCount.store(cnt + 1, std::memory_order_release);
        return true;
    }

    void InitReserve(size_t capacity) {
        kReserveCapacity = capacity > 0 ? capacity : kDefaultReserveCapacity;
        ReservePages.resize(kReserveCapacity, 0);
        ReserveCount.store(0, std::memory_order_relaxed);
    }

    bool ReserveLow() const {
        return ReserveCount.load(std::memory_order_acquire) < kReserveCapacity;
    }

    // --- Persistence bookkeeping (cold-path only in volatile mode) ---
    //
    // DESIGN DEBT: FrozenLock guards KeyNames and FrozenIndex, which are
    // ONLY needed for persistence recovery (looking up key strings from
    // h2 hashes after a crash/restart, and mapping frozen-page entries).
    //
    // In volatile mode (IsVolatile=true), NONE of this matters:
    //   - FrozenIndex is never read (Get's frozen-page fallback at ~line 1152
    //     is guarded by !Volatile)
    //   - KeyNames is only used to reconstruct key strings from h2 during
    //     eviction (swap fixup at ~line 484) and cold-page freeze (~line 1305)
    //   - Both eviction and cold-page freeze are themselves guarded by
    //     !Volatile already
    //
    // PROBLEM: Three FrozenLock acquisition sites are NOT guarded by Volatile:
    //   1. Set hot path (~line 1418): updates KeyNames[FrozenIndex] on every
    //      successful insert. This is the WORST offender — it runs on every
    //      Set even in volatile mode.
    //   2. EvictOneKey swap fixup (~line 484): looks up KeyNames when
    //      RemoveKeyEntry returns a swapped h2. In volatile mode this lookup
    //      is useless because KeyNames was never populated (if fixed).
    //   3. EvictOneKey cleanup (~line 515): erases from KeyNames/FrozenIndex.
    //      Same — no-op in volatile mode if KeyNames was never populated.
    //
    // INTERIM FIX: Guard all three sites with `if (!Volatile)`.
    //
    // PROPER FIX: The persistence subsystem should be decoupled from the
    // hot path entirely. Options:
    //   a) Separate VolatileDetails struct (no KeyNames/FrozenIndex/FrozenLock
    //      at all) — selected at construction time based on IsVolatile.
    //   b) Deferred batch: accumulate (h2, key) pairs in a lock-free buffer,
    //      flush to KeyNames periodically from a background thread.
    //   c) Concurrent hash map for KeyNames (folly::ConcurrentHashMap or
    //      similar) — eliminates the SpinLock but not the hash lookups.
    //
    struct FrozenEntry {
        std::string key;
        size_t pageIndex;
        uint32_t generation;
        size_t offset;
        size_t size;
    };
    std::unordered_map<uint64_t, FrozenEntry> FrozenIndex;
    std::unordered_map<uint64_t, std::string> KeyNames;
    SpinLock FrozenLock;

    alignas(64) std::atomic<unsigned int> NodeHitCount{0};
    alignas(64) std::atomic<size_t> NodeBytesRead{0};
    alignas(64) std::atomic<unsigned int> NodeLocalHitCount{0};
    alignas(64) std::atomic<size_t> NodeBytesWritten{0};

    PerNodeDetails(size_t arcCap, CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
        : NodePages(&nodeMR), KeyStore(arcCap, af, ff) { ShadowDesire.init(arcCap * 2); }
};

template<typename Policy>
struct PrivateNumaState{
    PerNodeDetails<Policy>** NodeDetails;
    AtomicRoundRobin rr;
    bool AllowNodeFallback = false;
};

template<typename Policy>
struct FurrBall<Policy>::ImplDetail {
    rocksdb::DB* db = nullptr;
    PrivateNumaState<Policy>* privateNumaState = nullptr;
};

// =====================================================================
//  Constructor
// =====================================================================

template<typename Policy>
NuAtlas::FurrBall<Policy>::FurrBall(const FurrConfig& config, size_t numPages) noexcept
    : cache(numPages),
    PageSize(config.PageSize),
    MaxPages(numPages),
    Volatile(config.IsVolatile),
    UseNUMA(config.EnableNUMA),
    ThreadLocalRoute(config.numaConfig ? config.numaConfig->UseThreadLocalRouting : false),
    DisableMigration(config.DisableMigration),
    policyConfig(Policy::MakeConfig(config.remarcConfig)),
    DataMembers(new ImplDetail())
{
}

// =====================================================================
//  Non-templated infrastructure
// =====================================================================

template<typename Policy>
Page* NuAtlas::FurrBall<Policy>::AllocatePage(size_t pageIndex) noexcept {
    void* memory = nullptr;
    if (UseNUMA) {
        memory = Numatic::AllocateOnNode(PageSize, Numatic::GetCurrentNode());
    }
    if (!memory) {
        if (UseNUMA && DataMembers->privateNumaState && !DataMembers->privateNumaState->AllowNodeFallback) {
            return nullptr;
        }
        memory = MemoryManager::AllocateMemory(PageSize);
    }

    Page* page = new Page(memory, PageSize, pageIndex);
    AllocatedPages.push_back(memory);
    Stats.TotalAllocated += PageSize;
    return page;
}

template<typename Policy>
void NuAtlas::FurrBall<Policy>::FlushPage(Page* page) noexcept {
    if (!page || !page->Data || Volatile) {
        return;
    }
    if (DataMembers && DataMembers->db) {
        std::string key(reinterpret_cast<const char*>(&page->PageIndex), sizeof(size_t));
        rocksdb::Slice value(reinterpret_cast<const char*>(page->Data), PageSize);
        rocksdb::Status status = DataMembers->db->Put(rocksdb::WriteOptions(), key, value);
        if (status.ok()) {
            page->Dirty = false;
            Stats.BytesWritten += PageSize;
        }
        else {
            Logger::getInstance().error("FlushPage failed for page " + std::to_string(page->PageIndex) + ": " + status.ToString());
        }
    }
}

template<typename Policy>
void NuAtlas::FurrBall<Policy>::OnEvict(const size_t& key, Page*& page) noexcept {
    if (!page) return;

    Stats.EvictionCount++;
    if (page->Dirty) {
        FlushPage(page);
    }
}

template<typename Policy>
void NuAtlas::FurrBall<Policy>::OnKeyEvict(int nodeID, const KeyMeta& meta) noexcept {
    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    if (!details || meta.PageIndex >= details->NodePages.size()) return;

    Page& page = details->NodePages[meta.PageIndex];
    page.CompactLock.lock();
    size_t idx = page.FindKeyIndex(meta.KeyHash);
    if (idx == SIZE_MAX) {
        page.CompactLock.unlock();
        return;
    }
    HashPair swapped = page.RemoveKeyEntryLocked(idx);
    page.CompactLock.unlock();

    size_t offset = reinterpret_cast<size_t>(meta.DataOffset) -
                    reinterpret_cast<size_t>(page.Data);
    page.AddFreeSlot(static_cast<uint32_t>(offset), static_cast<uint32_t>(meta.DataSize));

    if (swapped.h2 != 0) {
        details->KeyStore.UpdateInPlaceByHash(swapped,
            [idx](KeyMeta& m) {
                m.TempCtrlIdx = static_cast<uint8_t>(idx);
            });
    }

    if (page.ActiveKeys.load(std::memory_order_relaxed) == 0) {
        page.Recycle();
        page.Tier.store(PageTier::Hot, std::memory_order_release);
        Stats.PagesReclaimed.fetch_add(1, std::memory_order_relaxed);
    }

    Stats.EvictionCount.fetch_add(1, std::memory_order_relaxed);
    Stats.PerKeyEvictionCount.fetch_add(1, std::memory_order_relaxed);
}

template<typename Policy>
void* NuAtlas::FurrBall<Policy>::Get(void* vAddress) noexcept {
    if (!vAddress) return nullptr;

    size_t address = reinterpret_cast<size_t>(vAddress);
    size_t pageIndex = PageIndexForAddress(address);
    size_t offset = address - pageIndex;

    {
        std::shared_lock<std::shared_mutex> lock(rwMutex);

        if (cache.Contains(pageIndex)) {
            Page* page = cache.Get(pageIndex);
            if (page && page->Data) {
                Stats.HitCount++;
                Stats.BytesRead += PageSize;
                return static_cast<char*>(page->Data) + offset;
            }
        }
    }

    std::unique_lock<std::shared_mutex> lock(rwMutex);
    Stats.MissCount++;

    Page* page = AllocatePage(pageIndex);
    if (!page) {
        return nullptr;
    }

    if (DataMembers && DataMembers->db) {
        std::string key(reinterpret_cast<const char*>(&pageIndex), sizeof(size_t));
        std::string value;
        rocksdb::Status status = DataMembers->db->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok() && !value.empty()) {
            size_t copySize = std::min(value.size(), PageSize);
            std::memcpy(page->Data, value.data(), copySize);
            if (copySize < PageSize) {
                std::memset(static_cast<char*>(page->Data) + copySize, 0, PageSize - copySize);
            }
            Stats.BytesRead += copySize;
        }
    }

    page->Dirty = false;
    cache.Add(pageIndex, page);

    return static_cast<char*>(page->Data) + offset;
}

template<typename Policy>
bool NuAtlas::FurrBall<Policy>::Set(void* data, size_t size, size_t vAddress) noexcept {
    if (!data || size == 0) return false;

    size_t pageIndex = PageIndexForAddress(vAddress);
    size_t offset = vAddress - pageIndex;

    if (offset + size > PageSize) {
        Logger::getInstance().error("Set: data spans multiple pages (size=" + std::to_string(size) + " offset=" + std::to_string(offset) + ")");
        return false;
    }

    std::unique_lock<std::shared_mutex> lock(rwMutex);

    Page* page = nullptr;
    if (cache.Contains(pageIndex)) {
        page = cache.Get(pageIndex);
    }

    if (!page) {
        page = AllocatePage(pageIndex);
        if (!page) {
            return false;
        }
        cache.Add(pageIndex, page);
    }

    std::memcpy(static_cast<char*>(page->Data) + offset, data, size);
    page->Dirty = true;

    return true;
}

// =====================================================================
//  MigrateKey (policy-gated)
// =====================================================================

template<typename Policy>
bool NuAtlas::FurrBall<Policy>::MigrateKey(const std::string& key, const HashPair& hp, int sourceNode, int destNode) noexcept {
    if constexpr (!Policy::HasMigration) return false;

    if (sourceNode == destNode) return false;
    if (!DataMembers || !DataMembers->privateNumaState) return false;

    auto* srcDetails = DataMembers->privateNumaState->NodeDetails[sourceNode];
    auto* dstDetails = DataMembers->privateNumaState->NodeDetails[destNode];

    auto srcMeta = srcDetails->KeyStore.Find(key);
    if (!srcMeta.has_value()) return false;

    size_t dataSize = srcMeta->DataSize;
    if (dataSize == 0) return false;

    size_t dstPageIdx = dstDetails->CurrentPage.load(std::memory_order_relaxed);
    void* dstLoc = nullptr;
    for (size_t attempts = 0; attempts < dstDetails->NodePages.size(); attempts++) {
        if (dstPageIdx >= dstDetails->NodePages.size()) break;
        dstLoc = dstDetails->NodePages[dstPageIdx].TryBump(dataSize);
        if (dstLoc) break;
        size_t nextIdx = dstPageIdx + 1;
        if (nextIdx >= dstDetails->NodePages.size()) break;
        if (!dstDetails->CurrentPage.compare_exchange_weak(dstPageIdx, nextIdx,
                std::memory_order_relaxed, std::memory_order_relaxed)) {
            dstPageIdx = dstDetails->CurrentPage.load(std::memory_order_relaxed);
        } else {
            dstPageIdx = nextIdx;
        }
    }
    if (!dstLoc) return false;

    memcpy(dstLoc, srcMeta->DataOffset, dataSize);

    KeyMeta dstMeta;
    dstMeta.DataSize = dataSize;
    dstMeta.PageIndex = dstPageIdx;
    dstMeta.DataOffset = dstLoc;
    dstMeta.KeyHash = hp;

    Page& dstPage = dstDetails->NodePages[dstPageIdx];
    uint8_t initTC = Policy::InitialState();
    dstPage.AddKeyEntry(hp, initTC);
    dstMeta.TempCtrlIdx = static_cast<uint8_t>(dstPage.TempCtrl.size() - 1);

    Error insertErr = dstDetails->KeyStore.Set(key, dstMeta);
    if (insertErr != NO_ERR) {
        dstPage.RemoveKeyByHash(hp);
        return false;
    }

    auto erased = srcDetails->KeyStore.Erase(key);
    if (erased.err != NO_ERR) {
        dstDetails->KeyStore.Erase(key);
        dstPage.RemoveKeyByHash(hp);
        return false;
    }

    if (srcMeta->PageIndex < srcDetails->NodePages.size()) {
        Page& srcPage = srcDetails->NodePages[srcMeta->PageIndex];
        srcPage.CompactLock.lock();
        size_t idx = srcPage.FindKeyIndex(hp);
        HashPair swapped{};
        bool needSwapUpdate = false;
        if (idx != SIZE_MAX) {
            swapped = srcPage.RemoveKeyEntryLocked(idx);
            needSwapUpdate = (idx < srcPage.KeyH2.size() && swapped != hp);
        }
        srcPage.CompactLock.unlock();
        if (needSwapUpdate) {
            srcDetails->KeyStore.UpdateInPlaceByHash(swapped,
                [idx](KeyMeta& m) {
                m.TempCtrlIdx = static_cast<uint8_t>(idx);
            });
        }
    }

    Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
    Stats.BytesWritten.fetch_add(dataSize, std::memory_order_relaxed);
    return true;
}

// =====================================================================
//  EvictOneKey: per-key eviction for capacity pressure
//  Scans all Hot pages for the key with minimum EvictScore (REMARC)
//  or first-available key (non-REMARC), removes it, adds to free-list.
//  Returns the size of the freed slot, or 0 if nothing to evict.
// =====================================================================

template<typename Policy>
size_t NuAtlas::FurrBall<Policy>::EvictOneKey(int nodeID) noexcept {
    if (!DataMembers || !DataMembers->privateNumaState) return 0;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return 0;

    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    if (!details) return 0;

    size_t bestPageIdx = SIZE_MAX;
    size_t bestKeyIdx = SIZE_MAX;
    uint8_t bestScore = 0;

    for (size_t p = 0; p < details->NodePages.size(); p++) {
        Page& page = details->NodePages[p];
        if (page.Tier.load(std::memory_order_relaxed) != PageTier::Hot) continue;
        page.CompactLock.lock();
        size_t keyCount = page.TempCtrl.size();
        if (keyCount == 0) { page.CompactLock.unlock(); continue; }

        if constexpr (Policy::HasPerKeyState) {
            for (size_t k = 0; k < keyCount; k++) {
                uint8_t es = Policy::EvictScore(page.TempCtrl[k]);
                if (es > bestScore) {
                    bestScore = es;
                    bestPageIdx = p;
                    bestKeyIdx = k;
                }
            }
        } else {
            if (bestPageIdx == SIZE_MAX) {
                bestPageIdx = p;
                bestKeyIdx = 0;
            }
        }
        page.CompactLock.unlock();
    }

    if (bestPageIdx == SIZE_MAX || bestKeyIdx == SIZE_MAX) return 0;

    Page& page = details->NodePages[bestPageIdx];
    page.CompactLock.lock();
    if (bestKeyIdx >= page.KeyH2.size()) {
        page.CompactLock.unlock();
        return 0;
    }
    HashPair victimHash{page.KeyH1[bestKeyIdx], page.KeyH2[bestKeyIdx]};
    HashPair swapped = page.RemoveKeyEntryLocked(bestKeyIdx);
    bool needSwapUpdate = (!Volatile && swapped != victimHash && swapped.h2 != 0);
    page.CompactLock.unlock();

    if (needSwapUpdate) {
        details->FrozenLock.lock();
        auto it = details->KeyNames.find(swapped.h2);
        details->FrozenLock.unlock();
        if (it != details->KeyNames.end()) {
            details->KeyStore.UpdateInPlaceByHash(swapped,
                [idx = bestKeyIdx](KeyMeta& m) {
                    m.TempCtrlIdx = static_cast<uint8_t>(idx);
                });
        }
    }

    auto eraseResult = details->KeyStore.EraseByHash(victimHash);
    size_t freedSize = 0;
    if (eraseResult.err == NO_ERR && eraseResult.value.has_value()) {
        const KeyMeta& meta = eraseResult.value.value();
        size_t offset = reinterpret_cast<size_t>(meta.DataOffset) -
                        reinterpret_cast<size_t>(page.Data);
        size_t sz = meta.DataSize;
        page.AddFreeSlot(static_cast<uint32_t>(offset), static_cast<uint32_t>(sz));
        freedSize = sz;

        if (page.ActiveKeys.load(std::memory_order_relaxed) == 0) {
            page.Recycle();
            page.Tier.store(PageTier::Hot, std::memory_order_release);
            Stats.PagesReclaimed.fetch_add(1, std::memory_order_relaxed);
        }
    }

    Stats.EvictionCount.fetch_add(1, std::memory_order_relaxed);
    Stats.PerKeyEvictionCount.fetch_add(1, std::memory_order_relaxed);

    if (!Volatile) {
        details->FrozenLock.lock();
        details->KeyNames.erase(victimHash.h2);
        details->FrozenIndex.erase(victimHash.h2);
        details->FrozenLock.unlock();
    }

    return freedSize;
}

// =====================================================================
//  SyncNodeStats — aggregate per-node hot-path stats into global Stats
// =====================================================================

template<typename Policy>
void NuAtlas::FurrBall<Policy>::SyncNodeStats(int nodeID) noexcept {
    if (!DataMembers || !DataMembers->privateNumaState) return;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return;
    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    if (!details) return;

    Stats.HitCount.fetch_add(details->NodeHitCount.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    Stats.BytesRead.fetch_add(details->NodeBytesRead.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    Stats.LocalHitCount.fetch_add(details->NodeLocalHitCount.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    Stats.BytesWritten.fetch_add(details->NodeBytesWritten.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
}

// =====================================================================
//  ScanAndExecute (policy-gated)
// =====================================================================

template<typename Policy>
void NuAtlas::FurrBall<Policy>::ScanAndExecute(int nodeID) noexcept {
    if (!DataMembers || !DataMembers->privateNumaState) return;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return;

    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];

    // Pass 1: Process frozen pages
    for (size_t p = 0; p < details->NodePages.size(); p++) {
        Page& page = details->NodePages[p];
        if (page.Tier.load(std::memory_order_relaxed) != PageTier::Freeze) continue;

        if (page.Dirty) {
            FlushPage(&page);
        }

        page.CompactLock.lock();
        auto h1s = page.KeyH1;
        auto h2s = page.KeyH2;
        page.CompactLock.unlock();

        for (size_t ki = 0; ki < h2s.size(); ki++) {
            details->KeyStore.EraseByHash(HashPair{h1s[ki], h2s[ki]});
        }

        page.Recycle();
        Stats.EvictionCount.fetch_add(static_cast<unsigned int>(h2s.size()),
            std::memory_order_relaxed);
    }

    // Pass 2: Policy-specific scan
    if constexpr (Policy::HasScanner) {
        uint8_t maxEvictScore = 0;
        for (size_t p = 0; p < details->NodePages.size(); p++) {
            Page& page = details->NodePages[p];
            PageTier tier = page.Tier.load(std::memory_order_relaxed);
            if (tier == PageTier::Freeze || tier == PageTier::Empty) continue;
            size_t keyCount = page.TempCtrl.size();
            if (keyCount == 0) continue;

            for (size_t off = 0; off < keyCount; off++) {
                uint8_t tc = page.TempCtrl[off];
                Policy::TimeDecayKey(tc, policyConfig);
                page.TempCtrl[off] = tc;
                uint8_t eScore = Policy::EvictScore(tc);
                if (eScore > maxEvictScore) maxEvictScore = eScore;
            }

            uint32_t totalEnum = 0;
            for (size_t off = 0; off < keyCount; off += 32) {
                auto scan = Policy::ScanBatch(page.TempCtrl.data(), off, keyCount, policyConfig);
                totalEnum += scan.ePageNumSum;
            }

            float ePage = Policy::EPage(totalEnum, keyCount);
            if (ePage > static_cast<float>(policyConfig.ThetaEvict) / 30.0f) {
                page.TryTransition(PageTier::Hot, PageTier::Freeze);
            }
        }

        if constexpr (Policy::HasDesire) {
            details->MinDesire.store(maxEvictScore, std::memory_order_relaxed);
        }
    }
}

// =====================================================================
//  UpdateMinDesire: lightweight scan to enable migration gate
// =====================================================================

template<typename Policy>
void NuAtlas::FurrBall<Policy>::UpdateMinDesire(int nodeID) noexcept {
    if (!DataMembers || !DataMembers->privateNumaState) return;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return;
    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    if (!details) return;

    if constexpr (Policy::HasDesire && Policy::HasPerKeyState) {
        uint8_t maxES = 0;
        for (auto& page : details->NodePages) {
            PageTier tier = page.Tier.load(std::memory_order_relaxed);
            if (tier == PageTier::Empty || tier == PageTier::Freeze) continue;
            for (size_t i = 0; i < page.TempCtrl.size(); i++) {
                uint8_t es = Policy::EvictScore(page.TempCtrl[i]);
                if (es > maxES) maxES = es;
            }
        }
        details->MinDesire.store(maxES, std::memory_order_relaxed);
    } else if constexpr (Policy::HasDesire && !Policy::HasPerKeyState) {
        uint8_t maxD = 0;
        for (auto& page : details->NodePages) {
            PageTier tier = page.Tier.load(std::memory_order_relaxed);
            if (tier == PageTier::Empty || tier == PageTier::Freeze) continue;
            for (uint64_t h2 : page.KeyH2) {
                uint8_t d = details->KeyStore.GetDesire(h2);
                if (d > maxD) maxD = d;
            }
        }
        details->MinDesire.store(maxD, std::memory_order_relaxed);
    } else {
        for (auto& page : details->NodePages) {
            (void)page.Tier.load(std::memory_order_relaxed);
            if (!page.TempCtrl.empty()) (void)page.TempCtrl[0];
        }
    }
}

// =====================================================================
//  BackgroundEvict: per-key eviction on NodeJob maintenance thread
//  Replenishes reserve pages by evicting cold keys one at a time.
//  When a page empties naturally, it's recycled and pushed to reserve.
// =====================================================================

template<typename Policy>
void NuAtlas::FurrBall<Policy>::BackgroundEvict(int nodeID) noexcept {
    if (!DataMembers || !DataMembers->privateNumaState) return;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return;
    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    if (!details) return;

    auto relocateKey = [&](const HashPair& hash, size_t skipPage) -> bool {
        bool moved = false;
        details->KeyStore.UpdateInPlaceByHash(hash,
            [&](KeyMeta& meta) {
                size_t sz = meta.DataSize;
                void* src = meta.DataOffset;

                void* dst = nullptr;
                size_t dstIdx = SIZE_MAX;

                for (size_t p = 0; p < details->NodePages.size(); p++) {
                    if (p == skipPage) continue;
                    auto tier = details->NodePages[p].Tier.load(std::memory_order_relaxed);
                    if (tier != PageTier::Hot) continue;
                    dst = details->NodePages[p].TryBump(sz);
                    if (dst) { dstIdx = p; break; }
                }
                if (!dst) {
                    for (size_t p = 0; p < details->NodePages.size(); p++) {
                        if (p == skipPage) continue;
                        dst = details->NodePages[p].TryAllocFromFree(sz);
                        if (dst) { dstIdx = p; break; }
                    }
                }

                if (dst && src) {
                    memcpy(dst, src, sz);
                    uint8_t initTC = Policy::InitialState();
                    details->NodePages[dstIdx].AddKeyEntry(hash, initTC);
                    meta.PageIndex = dstIdx;
                    meta.DataOffset = dst;
                    meta.PageGeneration = details->NodePages[dstIdx].Generation;
                    meta.TempCtrlIdx = static_cast<uint8_t>(
                        details->NodePages[dstIdx].KeyH2.size() - 1);
                    moved = true;
                }
            });
        return moved;
    };

    // --- Phase 1: Staging relocation (reactive) ---
    if constexpr (Policy::HasStoreEviction) {
        size_t stagingIdx = details->StagingPageIdx.load(std::memory_order_relaxed);
        if (stagingIdx < details->NodePages.size()) {
            Page& stagingPage = details->NodePages[stagingIdx];
            if (stagingPage.Tier.load(std::memory_order_relaxed) == PageTier::Staging) {
                for (size_t i = 0; i < 64; i++) {
                    if (stagingPage.ActiveKeys.load(std::memory_order_relaxed) == 0) break;

                    stagingPage.CompactLock.lock();
                    if (stagingPage.KeyH2.empty()) {
                        stagingPage.CompactLock.unlock();
                        break;
                    }
                    HashPair stagingHash{stagingPage.KeyH1[0], stagingPage.KeyH2[0]};
                    stagingPage.CompactLock.unlock();

                    if (relocateKey(stagingHash, SIZE_MAX)) {
                        stagingPage.RemoveKeyByHash(stagingHash);
                    } else {
                        details->KeyStore.ForceEvictOne();
                    }
                }

                if (stagingPage.ActiveKeys.load(std::memory_order_relaxed) == 0
                    && stagingPage.GetUsedSize() > 0) {
                    stagingPage.Recycle();
                    stagingPage.Tier.store(PageTier::Staging, std::memory_order_release);
                }
            }
        }
    }

    // --- Phase 2: Reserve replenishment (reactive) ---
    while (details->ReserveLow()) {
        size_t freedSz = EvictOneKey(nodeID);
        if (freedSz == 0) break;

        for (size_t p = 0; p < details->NodePages.size(); p++) {
            Page& page = details->NodePages[p];
            if (page.Tier.load(std::memory_order_relaxed) == PageTier::Hot
                && page.ActiveKeys.load(std::memory_order_relaxed) == 0
                && page.GetUsedSize() == 0) {
                details->TryPushReserve(p);
                break;
            }
        }
    }

    // --- Phase 3: Page drain compaction (proactive) ---
    if constexpr (Policy::HasStoreEviction) {
        size_t drainTarget = SIZE_MAX;
        size_t worstDeadBytes = PageSize / 4;

        for (size_t p = 0; p < details->NodePages.size(); p++) {
            Page& page = details->NodePages[p];
            if (page.Tier.load(std::memory_order_relaxed) != PageTier::Hot) continue;
            if (p == details->StagingPageIdx.load(std::memory_order_relaxed)) continue;

            uint16_t keys = page.ActiveKeys.load(std::memory_order_relaxed);
            if (keys == 0 || keys > 4) continue;

            size_t dead = page.GetDeadBytes();
            if (dead > worstDeadBytes) {
                worstDeadBytes = dead;
                drainTarget = p;
            }
        }

        if (drainTarget != SIZE_MAX) {
            Page& srcPage = details->NodePages[drainTarget];

            while (srcPage.ActiveKeys.load(std::memory_order_relaxed) > 0) {
                srcPage.CompactLock.lock();
                if (srcPage.KeyH2.empty()) {
                    srcPage.CompactLock.unlock();
                    break;
                }
                HashPair hash{srcPage.KeyH1[0], srcPage.KeyH2[0]};
                srcPage.CompactLock.unlock();

                if (relocateKey(hash, drainTarget)) {
                    srcPage.RemoveKeyByHash(hash);
                } else {
                    details->KeyStore.ForceEvictOne();
                    break;
                }
            }

            if (srcPage.ActiveKeys.load(std::memory_order_relaxed) == 0) {
                srcPage.Recycle();
                srcPage.Tier.store(PageTier::Hot, std::memory_order_release);
                details->CurrentPage.store(drainTarget, std::memory_order_release);
            }
        }
    }

    // --- Phase 4: Time decay ---
    if constexpr (Policy::HasPerKeyState) {
        for (auto& page : details->NodePages) {
            PageTier tier = page.Tier.load(std::memory_order_relaxed);
            if (tier != PageTier::Hot) continue;
            for (size_t i = 0; i < page.TempCtrl.size(); i++) {
                uint8_t tc = page.TempCtrl[i];
                Policy::TimeDecayKey(tc, policyConfig);
                page.TempCtrl[i] = tc;
            }
        }
    }
}

// =====================================================================
//  ManagePages: REMARC-driven page eviction + key migration (simulated)
// =====================================================================

template<typename Policy>
typename FurrBall<Policy>::PageManageResult
FurrBall<Policy>::ManagePages(int nodeID, bool simulateIO) noexcept {
    PageManageResult result;
    if (!DataMembers || !DataMembers->privateNumaState) return result;
    if (nodeID < 0 || nodeID >= Detail::globalNumaState.NumaNodeCount) return result;

    auto* details = DataMembers->privateNumaState->NodeDetails[nodeID];
    auto t0 = std::chrono::high_resolution_clock::now();

    // Adaptive pressure gate: count free pages, skip scan if > 20% free
    size_t totalPages = details->NodePages.size();
    size_t freePages = 0;
    for (size_t p = 0; p < totalPages; p++) {
        PageTier tier = details->NodePages[p].Tier.load(std::memory_order_relaxed);
        if (tier == PageTier::Empty || tier == PageTier::Freeze || tier == PageTier::Dead) freePages++;
    }
    float pressure = 1.0f - static_cast<float>(freePages) / static_cast<float>(totalPages > 0 ? totalPages : 1);

    if constexpr (Policy::HasRemarcConfig) {
        size_t hotPages = 0;
        size_t deadPages = 0;
        for (size_t p = 0; p < totalPages; p++) {
            PageTier tier = details->NodePages[p].Tier.load(std::memory_order_relaxed);
            if (tier == PageTier::Hot) hotPages++;
            else if (tier == PageTier::Dead) deadPages++;
        }
        if (policyConfig.StaticEvictThresh < 0.0f) {
            uint8_t targetAge;
            if (pressure > 0.8f) {
                targetAge = 32;
            } else if (pressure > 0.5f) {
                targetAge = deadPages > 0 ? static_cast<uint8_t>(48) : static_cast<uint8_t>(64);
            } else {
                targetAge = deadPages > 0 ? static_cast<uint8_t>(96) : static_cast<uint8_t>(128);
            }
            if (details->MaxDeadAge < targetAge) {
                details->MaxDeadAge = std::min(static_cast<uint8_t>(details->MaxDeadAge + 2), targetAge);
            } else if (details->MaxDeadAge > targetAge) {
                details->MaxDeadAge = std::max(static_cast<uint8_t>(details->MaxDeadAge - 1), targetAge);
            }
        }
        (void)hotPages;
    }

    if (pressure < 0.2f) {
        auto t1 = std::chrono::high_resolution_clock::now();
        result.scanNs = std::chrono::duration<double, std::nano>(t1 - t0).count();
        return result;
    }

    if constexpr (Policy::HasScanner || Policy::HasDesire) {
        size_t prevDeadCleaned = 0;
        {
            size_t cleanupBudget = 8;
            for (size_t p = 0; p < details->NodePages.size() && prevDeadCleaned < cleanupBudget; p++) {
                Page& page = details->NodePages[p];
                if (page.Tier.load(std::memory_order_relaxed) != PageTier::Dead) continue;

                page.DeadAge++;
                if (page.DeadAge < details->MaxDeadAge) continue;

                page.CompactLock.lock();
                auto h1s = page.KeyH1;
                auto h2s = page.KeyH2;
                page.CompactLock.unlock();

                for (size_t ki = 0; ki < h2s.size(); ki++) {
                    details->KeyStore.EraseByHash(HashPair{h1s[ki], h2s[ki]});
                }
                page.Recycle();
                prevDeadCleaned++;
                result.pagesEvicted++;
                result.keysEvicted += h2s.size();
            }
        }

        if constexpr (Policy::HasDesire) {
            struct PageInfo {
                size_t idx;
                float coldFrac;
                size_t keyCount;
                uint64_t coldMask;
            };

            std::vector<PageInfo> scored;
            uint8_t globalMinDesire = details->MinDesire.load(std::memory_order_relaxed);
            (void)globalMinDesire;

            details->DesireLock.lock();
            for (size_t p = 0; p < details->NodePages.size(); p++) {
                Page& page = details->NodePages[p];
                PageTier tier = page.Tier.load(std::memory_order_relaxed);
                if (tier == PageTier::Empty || tier == PageTier::Freeze || tier == PageTier::Dead) continue;

                size_t keyCount = page.TempCtrl.size();
                if (keyCount == 0) continue;

                if constexpr (Policy::HasPerKeyState) {
                    for (size_t off = 0; off < keyCount; off++) {
                        uint8_t tc = page.TempCtrl[off];
                        Policy::TimeDecayKey(tc, policyConfig);
                        page.TempCtrl[off] = tc;
                    }
                }

                uint64_t coldMask = 0;
                if constexpr (Policy::HasPerKeyState) {
                    for (size_t k = 0; k < keyCount && k < 64; k++) {
                        uint8_t desire = details->ShadowDesire.get(page.KeyH2[k]);
                        if (Policy::EvictScore(desire) > 8) {
                            coldMask |= (1ULL << k);
                        }
                    }
                } else {
                    for (size_t k = 0; k < keyCount && k < 64; k++) {
                        uint8_t d = details->KeyStore.GetDesire(page.KeyH2[k]);
                        if (d < 3) {
                            coldMask |= (1ULL << k);
                        }
                    }
                }
                size_t coldCount = __builtin_popcountll(coldMask);
                float cf = (keyCount > 0) ? (float)coldCount / (float)keyCount : 0.0f;
                scored.push_back({p, cf, keyCount, coldMask});
            }
            details->DesireLock.unlock();

            std::sort(scored.begin(), scored.end(),
                [](const PageInfo& a, const PageInfo& b) { return a.coldFrac > b.coldFrac; });

            if (scored.empty()) goto done_scanner;

            if (!DisableMigration) {
            size_t maxMigrations = 1000;
            size_t migrated = 0;

            for (size_t ci = 0; ci < scored.size() && migrated < maxMigrations; ci++) {
                PageInfo& coldPI = scored[ci];
                uint64_t coldValid = (coldPI.keyCount >= 64) ? ~0ULL : ((1ULL << coldPI.keyCount) - 1);
                uint64_t hotOnCold = (~coldPI.coldMask) & coldValid;
                if (hotOnCold == 0) continue;

                for (size_t wi = scored.size() - 1; wi > ci && migrated < maxMigrations; wi--) {
                    PageInfo& warmPI = scored[wi];
                    uint64_t warmValid = (warmPI.keyCount >= 64) ? ~0ULL : ((1ULL << warmPI.keyCount) - 1);
                    uint64_t coldOnWarm = warmPI.coldMask & warmValid;
                    if (coldOnWarm == 0) continue;

                    int coldBit = __builtin_ctzll(coldOnWarm);
                    int hotBit  = __builtin_ctzll(hotOnCold);

                    Page& srcWarmPage = details->NodePages[warmPI.idx];
                    Page& srcColdPage = details->NodePages[coldPI.idx];

                    HashPair coldHash{srcWarmPage.KeyH1[coldBit], srcWarmPage.KeyH2[coldBit]};
                    uint8_t coldTc = srcWarmPage.TempCtrl[coldBit];
                    HashPair hotHash{srcColdPage.KeyH1[hotBit], srcColdPage.KeyH2[hotBit]};
                    uint8_t hotTc = srcColdPage.TempCtrl[hotBit];

                    HashPair swappedWarm = srcWarmPage.RemoveKeyEntry(coldBit);
                    if (swappedWarm != coldHash) {
                        details->KeyStore.UpdateInPlaceByHash(swappedWarm,
                            [idx = coldBit](KeyMeta& m) {
                                m.TempCtrlIdx = static_cast<uint8_t>(idx);
                            });
                    }

                    HashPair swappedCold = srcColdPage.RemoveKeyEntry(hotBit);
                    if (swappedCold != hotHash) {
                        details->KeyStore.UpdateInPlaceByHash(swappedCold,
                            [idx = hotBit](KeyMeta& m) {
                                m.TempCtrlIdx = static_cast<uint8_t>(idx);
                            });
                    }

                    srcWarmPage.AddKeyEntry(hotHash, hotTc);
                    details->KeyStore.UpdateInPlaceByHash(hotHash,
                        [pi = warmPI.idx, d = details](KeyMeta& m) {
                            m.PageIndex = pi;
                            m.TempCtrlIdx = static_cast<uint8_t>(
                                d->NodePages[pi].KeyH2.size() - 1);
                        });

                    srcColdPage.AddKeyEntry(coldHash, coldTc);
                    details->KeyStore.UpdateInPlaceByHash(coldHash,
                        [pi = coldPI.idx, d = details](KeyMeta& m) {
                            m.PageIndex = pi;
                            m.TempCtrlIdx = static_cast<uint8_t>(
                                d->NodePages[pi].KeyH2.size() - 1);
                        });

                    coldPI.keyCount = srcColdPage.KeyH2.size();
                    warmPI.keyCount = srcWarmPage.KeyH2.size();
                    migrated++;
                    result.keysMigrated++;
                    break;
                }
            }
            }

            if constexpr (!Policy::HasStoreEviction) {
                float& thresh = details->EvictThresh;
                if constexpr (Policy::HasRemarcConfig) {
                    if (policyConfig.StaticEvictThresh >= 0.0f)
                        thresh = policyConfig.StaticEvictThresh;
                }

                size_t evictBudget = std::max(size_t(1),
                    static_cast<size_t>(scored.size() * pressure * 0.25f));
                size_t newDeadKeys = 0;

                for (size_t i = 0; i < evictBudget && i < scored.size(); i++) {
                    if (scored[i].coldFrac < thresh) break;

                    Page& page = details->NodePages[scored[i].idx];
                    if (!simulateIO && page.Dirty) FlushPage(&page);
                    page.Tier.store(PageTier::Dead, std::memory_order_release);
                    page.DeadAge = 1;
                    result.pagesEvicted++;
                    result.keysEvicted += scored[i].keyCount;
                    newDeadKeys += scored[i].keyCount;
                }

                if constexpr (Policy::HasRemarcConfig) {
                    if (policyConfig.StaticEvictThresh < 0.0f) {
                        size_t revivals = Stats.GetMigrationCount() - details->PrevMigrationCount;
                        details->PrevMigrationCount = Stats.GetMigrationCount();
                        if (details->PrevDeadKeys > 0) {
                            float revivalRate = static_cast<float>(revivals) / static_cast<float>(details->PrevDeadKeys);
                            float error = revivalRate - 0.15f;
                            details->EvictThresh += details->AdaptiveStep * error;
                            details->EvictThresh = std::clamp(details->EvictThresh, 0.1f, 0.9f);
                        } else if (newDeadKeys == 0 && pressure > 0.5f) {
                            details->EvictThresh = std::max(details->EvictThresh - 0.1f, 0.1f);
                        }
                        details->AdaptiveStep = std::clamp(0.05f + 0.05f * pressure, 0.02f, 0.15f);
                        details->PrevDeadKeys = newDeadKeys;
                    }
                } else {
                    if (details->PrevDeadKeys > 0) {
                        size_t revivals = Stats.GetMigrationCount() - details->PrevMigrationCount;
                        details->PrevMigrationCount = Stats.GetMigrationCount();
                        float revivalRate = static_cast<float>(revivals) / static_cast<float>(details->PrevDeadKeys);
                        float error = revivalRate - 0.15f;
                        details->EvictThresh += details->AdaptiveStep * error;
                        details->EvictThresh = std::clamp(details->EvictThresh, 0.1f, 0.9f);
                    } else if (newDeadKeys == 0 && pressure > 0.5f) {
                        details->EvictThresh = std::max(details->EvictThresh - 0.1f, 0.1f);
                    }
                    details->AdaptiveStep = std::clamp(0.05f + 0.05f * pressure, 0.02f, 0.15f);
                    details->PrevDeadKeys = newDeadKeys;
                }
            }
            } // !HasStoreEviction
        done_scanner:;
    }

    if constexpr (Policy::HasScanner && !Policy::HasDesire) {
            static constexpr uint8_t COLD_BIT_THRESHOLD = 5;

            struct PageInfo {
                size_t idx;
                float coldFrac;
                size_t keyCount;
                uint64_t coldMask;
            };

            std::vector<PageInfo> scored;

            for (size_t p = 0; p < details->NodePages.size(); p++) {
                Page& page = details->NodePages[p];
                PageTier tier = page.Tier.load(std::memory_order_relaxed);
                if (tier == PageTier::Empty || tier == PageTier::Freeze || tier == PageTier::Dead) continue;

                size_t keyCount = page.TempCtrl.size();
                if (keyCount == 0) continue;

                uint64_t coldMask = 0;
                for (size_t k = 0; k < keyCount && k < 64; k++) {
                    uint8_t tc = page.TempCtrl[k];
                    Policy::TimeDecayKey(tc, policyConfig);
                    page.TempCtrl[k] = tc;
                    if (Policy::EvictScore(tc) > COLD_BIT_THRESHOLD) {
                        coldMask |= (1ULL << k);
                    }
                }
                size_t coldCount = __builtin_popcountll(coldMask);
                float cf = (float)coldCount / (float)keyCount;
                scored.push_back({p, cf, keyCount, coldMask});
            }

            std::sort(scored.begin(), scored.end(),
                [](const PageInfo& a, const PageInfo& b) { return a.coldFrac > b.coldFrac; });

            if (scored.empty()) return result;

            {
                size_t evictBudget = std::max(size_t(1), scored.size() / 20);
                for (size_t i = 0; i < evictBudget && i < scored.size(); i++) {
                    if (scored[i].coldFrac < 0.1f) break;

                    Page& page = details->NodePages[scored[i].idx];
                    if (!simulateIO && page.Dirty) FlushPage(&page);
                    page.Tier.store(PageTier::Dead, std::memory_order_release);
                    page.DeadAge = 1;
                    result.pagesEvicted++;
                    result.keysEvicted += scored[i].keyCount;
                }
            }
    }

    if constexpr (Policy::HasDesire && Policy::HasPerKeyState) {
        uint8_t maxES = 0;
        for (auto& page : details->NodePages) {
            for (auto tc : page.TempCtrl) {
                uint8_t es = Policy::EvictScore(tc);
                if (es > maxES) maxES = es;
            }
        }
        details->MinDesire.store(maxES, std::memory_order_relaxed);
    } else if constexpr (Policy::HasDesire && !Policy::HasPerKeyState) {
        uint8_t maxD = 0;
        for (auto& page : details->NodePages) {
            for (uint64_t h2 : page.KeyH2) {
                uint8_t d = details->KeyStore.GetDesire(h2);
                if (d > maxD) maxD = d;
            }
        }
        details->MinDesire.store(maxD, std::memory_order_relaxed);
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    result.scanNs = std::chrono::duration<double, std::nano>(t1 - t0).count();
    return result;
}

// =====================================================================
//  Get (string key, NUMA-aware)
// =====================================================================

template<typename Policy>
Error NuAtlas::FurrBall<Policy>::Get(const std::string &key, void* outBuf, size_t BufSize, size_t& outSize) noexcept
{
    if(key.empty()){
        Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
        return INVALID_ARG;
    }

    int nodeCount = Detail::globalNumaState.NumaNodeCount;
    int currentNode = Numatic::GetCurrentNode();
    HashPair hp = HashKey(key);

    auto tryNode = [&](int n) -> Error {
        auto* details = DataMembers->privateNumaState->NodeDetails[n];
        auto meta = details->KeyStore.Find(key);
        if(meta.has_value()){
            if(meta->PageIndex < details->NodePages.size()) {
                Page& deadPage = details->NodePages[meta->PageIndex];
                PageTier tier = deadPage.Tier.load(std::memory_order_acquire);
                if (tier == PageTier::Dead) {
                    if (meta->PageGeneration == deadPage.Generation) {
                        outSize = meta->DataSize;
                        if(BufSize < meta->DataSize) return BUF_NOT_LARGE_ENOUGH;
                        memcpy(outBuf, meta->DataOffset, meta->DataSize);

                        size_t hotIdx = deadPage.FindKeyIndex(hp);
                        if (hotIdx == SIZE_MAX) {
                            details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
                            details->NodeBytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
                            return NO_ERR;
                        }

                        uint8_t hotTc = (hotIdx < deadPage.TempCtrl.size()) ? deadPage.TempCtrl[hotIdx] : 0;

                        int bestPage = -1;
                        size_t bestIdx = SIZE_MAX;
                        uint8_t bestScore = 0;
                        for (size_t wp = 0; wp < details->NodePages.size(); wp++) {
                            Page& warmPage = details->NodePages[wp];
                            PageTier wt = warmPage.Tier.load(std::memory_order_relaxed);
                            if (wt != PageTier::Hot) continue;
                            for (size_t wi = 0; wi < warmPage.KeyH2.size(); wi++) {
                                uint8_t sc = 0;
                                if constexpr (Policy::HasPerKeyState) {
                                    uint8_t tc = (wi < warmPage.TempCtrl.size()) ? warmPage.TempCtrl[wi] : 0;
                                    sc = Policy::EvictScore(tc);
                                } else {
                                    sc = details->KeyStore.GetDesire(warmPage.KeyH2[wi]);
                                    sc = 15 - sc;
                                }
                                if (sc > bestScore) {
                                    bestScore = sc;
                                    bestPage = static_cast<int>(wp);
                                    bestIdx = wi;
                                }
                            }
                        }

                        if (bestPage >= 0) {
                            Page& warmPage = details->NodePages[bestPage];
                            HashPair coldHash{warmPage.KeyH1[bestIdx], warmPage.KeyH2[bestIdx]};

                            HashPair swappedHot = deadPage.RemoveKeyEntry(hotIdx);
                            if (swappedHot != hp) {
                                details->KeyStore.UpdateInPlaceByHash(swappedHot,
                                    [hIdx = hotIdx](KeyMeta& m) {
                                        m.TempCtrlIdx = static_cast<uint8_t>(hIdx);
                                    });
                            }

                            HashPair swappedCold = warmPage.RemoveKeyEntry(bestIdx);
                            if (swappedCold != coldHash) {
                                details->KeyStore.UpdateInPlaceByHash(swappedCold,
                                    [bIdx = bestIdx](KeyMeta& m) {
                                        m.TempCtrlIdx = static_cast<uint8_t>(bIdx);
                                    });
                            }

                            details->KeyStore.EraseByHash(coldHash);
                            if (DataMembers && DataMembers->db) {
                                std::string rkey(reinterpret_cast<const char*>(&coldHash.h2), sizeof(uint64_t));
                            }

                            warmPage.AddKeyEntry(hp, hotTc);
                            details->KeyStore.UpdateInPlaceByHash(hp,
                                [pi = static_cast<size_t>(bestPage), d = details](KeyMeta& m) {
                                    m.PageIndex = pi;
                                    m.TempCtrlIdx = static_cast<uint8_t>(
                                        d->NodePages[pi].KeyH2.size() - 1);
                                    m.PageGeneration = d->NodePages[pi].Generation;
                                });

                            Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
                            Stats.EvictionCount.fetch_add(1, std::memory_order_relaxed);
                        } else {
                            deadPage.Tier.store(PageTier::Hot, std::memory_order_release);
                            deadPage.DeadAge = 0;
                            Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
                        }

                        details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
                        details->NodeBytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
                        return NO_ERR;
                    }
                    deadPage.RemoveKeyByHash(hp);
                    details->KeyStore.EraseByHash(hp);
                    return INVALID_ARG;
                }
                if (tier == PageTier::Cold) {
                    if (meta->PageGeneration == deadPage.Generation) {
                        outSize = meta->DataSize;
                        if(BufSize < meta->DataSize) return BUF_NOT_LARGE_ENOUGH;
                        memcpy(outBuf, meta->DataOffset, meta->DataSize);
                        details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
                        details->NodeBytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
                        Stats.ColdPageHits.fetch_add(1, std::memory_order_relaxed);
                        return NO_ERR;
                    }
                    deadPage.RemoveKeyByHash(hp);
                    details->KeyStore.EraseByHash(hp);
                    return INVALID_ARG;
                }
            }
#ifdef SIMULATE_NUMA_LATENCY_NS
            if(currentNode != n) {
                simulateCrossNodeLatency();
            }
#endif
            if constexpr (Policy::HasPerKeyState) {
                if(meta->PageIndex < details->NodePages.size()) {
                    Page& page = details->NodePages[meta->PageIndex];
                    if(meta->TempCtrlIdx < page.TempCtrl.size()) {
                        bool isRemote = (currentNode != n);
                        uint8_t tc = page.TempCtrl[meta->TempCtrlIdx];

                        if constexpr (Policy::HasDesire) {
                            auto* localDetails = DataMembers->privateNumaState->NodeDetails[currentNode];
                            uint8_t desire = localDetails->ShadowDesire.get(hp.h2);
                            desire = Policy::OnLocalAccess(desire, policyConfig);
                            localDetails->ShadowDesire.set(hp.h2, desire);

                            if(isRemote) {
                                uint8_t worstEvict = localDetails->MinDesire.load(std::memory_order_relaxed);
                                if(Policy::EvictScore(desire) < worstEvict) {
                                    MigrateKey(key, hp, n, currentNode);
                                    outSize = meta->DataSize;
                                    if(BufSize < meta->DataSize) return BUF_NOT_LARGE_ENOUGH;
                                    memcpy(outBuf, meta->DataOffset, meta->DataSize);
                                    details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
                                    details->NodeBytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
                                    return NO_ERR;
                                }
                            }
                        }

                        tc = isRemote ? Policy::OnRemoteAccess(tc, policyConfig)
                                      : Policy::OnLocalAccess(tc, policyConfig);
                        page.TempCtrl[meta->TempCtrlIdx] = tc;

                        if constexpr (Policy::HasMigration) {
                            if(isRemote && Policy::ShouldHotNode(tc, policyConfig)) {
                                details->KeyStore.UpdateInPlace(key, [](KeyMeta&) {
                                });
                            }
                        }
                    }
                }
            }
            outSize = meta->DataSize;
            if(BufSize < meta->DataSize) return BUF_NOT_LARGE_ENOUGH;
            memcpy(outBuf, meta->DataOffset, meta->DataSize);
            details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
            details->NodeBytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
            return NO_ERR;
        }
        if (!Volatile && DataMembers && DataMembers->db) {
            details->FrozenLock.lock();
            auto fit = details->FrozenIndex.find(hp.h2);
            if (fit != details->FrozenIndex.end()) {
                const auto& fe = fit->second;
                std::string slabKey = "frozen:" + std::to_string(fe.pageIndex);
                std::string slabData;
                auto s = DataMembers->db->Get(rocksdb::ReadOptions(), slabKey, &slabData);
                if (s.ok() && slabData.size() >= fe.offset + fe.size) {
                    outSize = fe.size;
                    if (BufSize < fe.size) {
                        details->FrozenLock.unlock();
                        return BUF_NOT_LARGE_ENOUGH;
                    }
                    memcpy(outBuf, slabData.data() + fe.offset, fe.size);
                    details->FrozenLock.unlock();
                    details->NodeHitCount.fetch_add(1, std::memory_order_relaxed);
                    details->NodeBytesRead.fetch_add(fe.size, std::memory_order_relaxed);
                    Stats.FrozenPageHits.fetch_add(1, std::memory_order_relaxed);
                    return NO_ERR;
                }
            }
            details->FrozenLock.unlock();
        }
        return INVALID_ARG;
    };

    if(ThreadLocalRoute){
        int local = currentNode;
        Error err = tryNode(local);
        if(err == NO_ERR){
            DataMembers->privateNumaState->NodeDetails[local]->NodeLocalHitCount.fetch_add(1, std::memory_order_relaxed);
            return NO_ERR;
        }
        for(int n = 0; n < nodeCount; n++){
            if(n == local) continue;
            err = tryNode(n);
            if(err == NO_ERR) return NO_ERR;
        }
    }else{
        for(int n = 0; n < nodeCount; n++){
            Error err = tryNode(n);
            if(err == NO_ERR) return NO_ERR;
        }
    }

    Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
    return INVALID_ARG;
}

// =====================================================================
//  Set (string key, NUMA-aware)
// =====================================================================

template<typename Policy>
Error NuAtlas::FurrBall<Policy>::Set(const std::string &key, void *data, size_t size) noexcept
{
    if(!data || !size){
        Logger::getInstance().error("Furrball::Set was called with invalid arguments");
        return INVALID_ARG;
    }

    int targetNode = ThreadLocalRoute ? Numatic::GetCurrentNode() : this->DataMembers->privateNumaState->rr.Get();
    auto* details = DataMembers->privateNumaState->NodeDetails[targetNode];

    auto existing = details->KeyStore.Find(key);
    if(existing.has_value() && size <= existing->DataSize){
        Error err = details->KeyStore.UpdateInPlace(key, [&data, &size, this, targetNode, &details](KeyMeta& meta){
            memcpy(meta.DataOffset, data, size);
            meta.DataSize = size;
            if constexpr (Policy::HasPerKeyState) {
                if(meta.PageIndex < details->NodePages.size()) {
                    Page& page = details->NodePages[meta.PageIndex];
                    if(meta.TempCtrlIdx < page.TempCtrl.size()) {
                        int currentNode = Numatic::GetCurrentNode();
                        bool isRemote = (currentNode != targetNode);
                        uint8_t tc = page.TempCtrl[meta.TempCtrlIdx];
                        tc = isRemote ? Policy::OnRemoteAccess(tc, policyConfig)
                                      : Policy::OnLocalAccess(tc, policyConfig);
                        page.TempCtrl[meta.TempCtrlIdx] = tc;
                    }
                }
            }
        });
        if(err == NO_ERR){
            details->NodeBytesWritten.fetch_add(size, std::memory_order_relaxed);
            return NO_ERR;
        }
    }

    size_t pageIdx = details->CurrentPage.load(std::memory_order_relaxed);
    void* Loc = nullptr;
    [[maybe_unused]] bool didRotate = false;

    while (true) {
        if (pageIdx >= details->NodePages.size()) {
            if constexpr (!Policy::HasStoreEviction) {
                size_t numPgs = details->NodePages.size();

                if (!didRotate) {
                    size_t sacrificeIdx = SIZE_MAX;

                    if constexpr (Policy::HasPerKeyState) {
                            uint8_t minPageMaxEvict = UINT8_MAX;
                            for (size_t p = 0; p < numPgs; p++) {
                                Page& pg = details->NodePages[p];
                                if (pg.Tier.load(std::memory_order_relaxed) != PageTier::Hot) continue;
                                if (pg.KeyH2.empty()) continue;
                                uint8_t pageMax = 0;
                                for (size_t k = 0; k < pg.TempCtrl.size(); k++) {
                                    uint8_t es = Policy::EvictScore(pg.TempCtrl[k]);
                                    if (es > pageMax) pageMax = es;
                                }
                                if (pageMax < minPageMaxEvict) {
                                    minPageMaxEvict = pageMax;
                                    sacrificeIdx = p;
                                }
                            }
                        } else {
                            for (size_t p = 0; p < numPgs; p++) {
                                if (details->NodePages[p].Tier.load(std::memory_order_relaxed) == PageTier::Hot
                                    && !details->NodePages[p].KeyH2.empty()) {
                                    sacrificeIdx = p;
                                    break;
                                }
                            }
                        }

                        if (sacrificeIdx != SIZE_MAX) {
                            details->NodePages[sacrificeIdx].Tier.store(PageTier::Cold, std::memory_order_release);
                            Stats.EvictionCount.fetch_add(1, std::memory_order_relaxed);
                            Stats.ColdPageRotations.fetch_add(1, std::memory_order_relaxed);

                            bool rotated = false;
                            for (size_t p = 0; p < numPgs && !rotated; p++) {
                                if (p == sacrificeIdx) continue;
                                if (details->NodePages[p].Tier.load(std::memory_order_relaxed) != PageTier::Cold) continue;

                                Page& coldPage = details->NodePages[p];
                                if (!Volatile && coldPage.Dirty) FlushPage(&coldPage);

                                coldPage.CompactLock.lock();
                                auto h1s = coldPage.KeyH1;
                                auto h2s = coldPage.KeyH2;
                                coldPage.CompactLock.unlock();

                                if (!Volatile && DataMembers && DataMembers->db) {
                                    std::string slabKey = "frozen:" + std::to_string(p);
                                    rocksdb::Slice slabVal(
                                        reinterpret_cast<const char*>(coldPage.Data),
                                        coldPage.GetUsedSize());
                                    DataMembers->db->Put(rocksdb::WriteOptions(), slabKey, slabVal);
                                    details->NodeBytesWritten.fetch_add(coldPage.GetUsedSize(), std::memory_order_relaxed);

                                    details->FrozenLock.lock();
                                    for (size_t ki = 0; ki < h2s.size(); ki++) {
                                        auto it = details->KeyNames.find(h2s[ki]);
                                        if (it == details->KeyNames.end()) continue;
                                        auto meta = details->KeyStore.Find(it->second);
                                        if (!meta.has_value()) continue;
                                        if (meta->PageIndex != static_cast<size_t>(p)) continue;
                                        if (meta->PageGeneration != coldPage.Generation) continue;
                                        typename PerNodeDetails<Policy>::FrozenEntry fe;
                                        fe.key = it->second;
                                        fe.pageIndex = p;
                                        fe.generation = coldPage.Generation;
                                        fe.offset = reinterpret_cast<size_t>(meta->DataOffset) -
                                                    reinterpret_cast<size_t>(coldPage.Data);
                                        fe.size = meta->DataSize;
                                        details->FrozenIndex[h2s[ki]] = std::move(fe);
                                    }
                                    details->FrozenLock.unlock();

                                    Stats.FrozenPagesPersisted.fetch_add(1, std::memory_order_relaxed);
                                }

                                for (size_t ki = 0; ki < h2s.size(); ki++) {
                                    details->KeyStore.EraseByHash(HashPair{h1s[ki], h2s[ki]});
                                }

                                coldPage.Recycle();
                                coldPage.Tier.store(PageTier::Hot, std::memory_order_release);
                                Stats.PagesReclaimed.fetch_add(1, std::memory_order_relaxed);

                                details->CurrentPage.store(p, std::memory_order_relaxed);
                                pageIdx = p;
                                rotated = true;
                            }
                            if (rotated) {
                                didRotate = true;
                                continue;
                        }
                    }
                }

                for (size_t dp = 0; dp < numPgs; dp++) {
                    Page& deadPage = details->NodePages[dp];
                    if (deadPage.Tier.load(std::memory_order_acquire) != PageTier::Dead) continue;
                    if (deadPage.ActiveKeys.load(std::memory_order_acquire) != 0) continue;
                    deadPage.Recycle();
                    deadPage.Tier.store(PageTier::Hot, std::memory_order_release);
                    Stats.PagesReclaimed.fetch_add(1, std::memory_order_relaxed);
                    details->CurrentPage.store(dp, std::memory_order_relaxed);
                    pageIdx = dp;
                    Loc = deadPage.TryBump(size);
                    if (Loc) break;
                }
                if (Loc) break;
            }
            return OUT_OF_MEM;
        }
        Loc = details->NodePages[pageIdx].TryBump(size);
        if (Loc) break;
        if constexpr (!Policy::HasStoreEviction) {
            for (size_t p = 0; p < details->NodePages.size(); p++) {
                Loc = details->NodePages[p].TryAllocFromFree(size);
                if (Loc) { pageIdx = p; break; }
            }
            if (Loc) break;
        }
        size_t nextIdx = pageIdx + 1;
        if constexpr (Policy::HasStoreEviction) {
            while (nextIdx < details->NodePages.size() &&
                   details->NodePages[nextIdx].Tier.load(std::memory_order_relaxed) == PageTier::Staging) {
                nextIdx++;
            }
        }
        if (nextIdx >= details->NodePages.size()) {
            if constexpr (Policy::HasStoreEviction) {
                size_t stagingIdx = details->StagingPageIdx.load(std::memory_order_relaxed);
                if (stagingIdx < details->NodePages.size()) {
                    Loc = details->NodePages[stagingIdx].TryBump(size);
                    if (Loc) pageIdx = stagingIdx;
                }
                if (!Loc) {
                    bool evicted = details->KeyStore.ForceEvictOne();
                    if (evicted) {
                        for (size_t p = 0; p < details->NodePages.size(); p++) {
                            Loc = details->NodePages[p].TryAllocFromFree(size);
                            if (Loc) { pageIdx = p; break; }
                        }
                    }
                }
                if (!Loc) return OUT_OF_MEM;
                memcpy(Loc, data, size);

                Page& page = details->NodePages[pageIdx];
                HashPair hp = HashKey(key);
                uint8_t initTC = Policy::InitialState();
                page.AddKeyEntry(hp, initTC);

                KeyMeta metadata;
                metadata.DataSize = size;
                metadata.PageIndex = pageIdx;
                metadata.DataOffset = Loc;
                metadata.PageGeneration = page.Generation;
                metadata.KeyHash = hp;
                metadata.TempCtrlIdx = static_cast<uint8_t>(page.TempCtrl.size() - 1);

                Error err = details->KeyStore.Set(key, metadata);
                if (err == NO_ERR) {
                    details->NodeBytesWritten.fetch_add(size, std::memory_order_relaxed);
                    if (!Volatile) {
                        details->FrozenLock.lock();
                        details->KeyNames[hp.h2] = key;
                        details->FrozenIndex.erase(hp.h2);
                        details->FrozenLock.unlock();
                    }
                    if constexpr (Policy::HasDesire && Policy::HasPerKeyState) {
                        uint8_t desire = details->ShadowDesire.get(hp.h2);
                        desire = Policy::OnLocalAccess(desire, policyConfig);
                        details->ShadowDesire.set(hp.h2, desire);
                    }
                } else {
                    page.RemoveKeyByHash(hp);
                }
                if (Detail::globalNumaState.Workers)
                    Detail::globalNumaState.Workers[targetNode].WakeMaintenance();
                return err;
            }
            size_t reserveIdx;
            if (details->TryPopReserve(reserveIdx)) {
                pageIdx = reserveIdx;
                Loc = details->NodePages[pageIdx].TryBump(size);
                if (Loc) {
                    if (Detail::globalNumaState.Workers)
                        Detail::globalNumaState.Workers[targetNode].WakeMaintenance();
                    break;
                }
            }
            size_t evictedSz = EvictOneKey(targetNode);
            if (evictedSz > 0) {
                for (size_t r = 0; r < details->NodePages.size(); r++) {
                    Loc = details->NodePages[r].TryAllocFromFree(size);
                    if (Loc) { pageIdx = r; break; }
                }
            }
            if (!Loc) {
                pageIdx = details->NodePages.size();
                continue;
            }
            break;
        }
        if (!details->CurrentPage.compare_exchange_weak(pageIdx, nextIdx, std::memory_order_relaxed)) {
            pageIdx = details->CurrentPage.load(std::memory_order_relaxed);
        } else {
            pageIdx = nextIdx;
        }
    }
    memcpy(Loc, data, size);

    Page& page = details->NodePages[pageIdx];
    HashPair hp = HashKey(key);
    uint8_t initTC = Policy::InitialState();
    page.AddKeyEntry(hp, initTC);

    KeyMeta metadata;
    metadata.DataSize = size;
    metadata.PageIndex = pageIdx;
    metadata.DataOffset = Loc;
    metadata.PageGeneration = page.Generation;
    metadata.KeyHash = hp;
    metadata.TempCtrlIdx = static_cast<uint8_t>(page.TempCtrl.size() - 1);

    Error err = details->KeyStore.Set(key, metadata);
    if (err == NO_ERR) {
        details->NodeBytesWritten.fetch_add(size, std::memory_order_relaxed);
        if (!Volatile) {
            details->FrozenLock.lock();
            details->KeyNames[hp.h2] = key;
            details->FrozenIndex.erase(hp.h2);
            details->FrozenLock.unlock();
        }
        if constexpr (Policy::HasDesire && Policy::HasPerKeyState) {
            uint8_t desire = details->ShadowDesire.get(hp.h2);
            desire = Policy::OnLocalAccess(desire, policyConfig);
            details->ShadowDesire.set(hp.h2, desire);
        }
    } else {
        page.RemoveKeyByHash(hp);
    }
    return err;
}

// =====================================================================
//  Bootstrap / Shutdown / CreateBall / Destructor
// =====================================================================

template<typename Policy>
void NuAtlas::FurrBall<Policy>::Bootstrap()
{
    auto& gs = Detail::globalNumaState;
    if (gs.Initialized) return;
    gs.Initialized = true;

    if (Numatic::IsNUMAAvailable()) {
        gs.NumaNodeCount = Numatic::GetNodeCount();
        gs.SysNumaPageSize = Numatic::GetNodePageSize();
        gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * gs.NumaNodeCount);
        Logger::getInstance().info("Creating Node Workers");
        for (int i = 0; i < gs.NumaNodeCount; i++) {
            new(&gs.Workers[i]) NodeJob(i);
            gs.Workers[i].Start([=](){
                Logger::getInstance().info(std::string("Create a node worker pinned on NumaID: ") + std::to_string(i));
            });
        }
    }
}

template<typename Policy>
void NuAtlas::FurrBall<Policy>::Shutdown()
{
    for (auto &&fb : OpenBalls)
    {
        delete fb;
    }
    OpenBalls.clear();

    auto& gs = Detail::globalNumaState;
    if (!gs.Initialized) return;
    gs.Initialized = false;

    for(int i = 0; i < gs.NumaNodeCount; i++){
        if (gs.Workers) gs.Workers[i].Stop();
    }
    for(int i = 0; i < gs.NumaNodeCount; i++){
        if (gs.Workers) gs.Workers[i].~NodeJob();
    }
    free(gs.Workers);
    gs = {};
}

template<typename Policy>
FurrBall<Policy> *FurrBall<Policy>::CreateBall(const std::string &DBpath, const FurrConfig &config, bool overwrite) noexcept
{
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    PrivateNumaState<Policy>* pNumaState = nullptr;
    FurrBall* fb = nullptr;
    size_t numPages = 0;
    size_t totalPhysicalPageSize = 0;

    rocksdb::BlockBasedTableOptions tableOptions;
    tableOptions.block_cache = rocksdb::NewLRUCache(0);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOptions));
    options.create_if_missing = true;
    options.optimize_filters_for_hits = true;

#if ROCKSDB_MAJOR >= 11
    std::unique_ptr<rocksdb::DB> dbPtr;
    auto status = rocksdb::DB::Open(options, DBpath, &dbPtr);
    if (!status.ok()) {
        Logger::getInstance().error("Failed to open RocksDB at " + DBpath + ": " + status.ToString());
        return nullptr;
    }
    db = dbPtr.release();
#else
    rocksdb::DB* rawDb = nullptr;
    auto status = rocksdb::DB::Open(options, DBpath, &rawDb);
    if (!status.ok()) {
        Logger::getInstance().error("Failed to open RocksDB at " + DBpath + ": " + status.ToString());
        return nullptr;
    }
    db = rawDb;
#endif

    numPages = config.InitialPageCount;
    totalPhysicalPageSize = numPages * config.PageSize;

    if(config.EnableNUMA == true && Detail::globalNumaState.NumaNodeCount){
        if(!config.numaConfig){
            goto Exit;
        }
        pNumaState = new PrivateNumaState<Policy>();
        int nodeCount = Detail::globalNumaState.NumaNodeCount;
        pNumaState->NodeDetails = (PerNodeDetails<Policy>**)malloc(sizeof(PerNodeDetails<Policy>*) * nodeCount);
        pNumaState->rr.SetN(nodeCount);
        pNumaState->AllowNodeFallback = config.numaConfig->AllowNodeFallback;

        std::vector<size_t> nodePageCounts;
        if (!config.PerNodePages.empty()) {
            nodePageCounts = config.PerNodePages;
            nodePageCounts.resize(nodeCount, config.InitialPageCount);
        } else if (config.TotalCapacityBytes > 0) {
            size_t perNodeBytes = config.TotalCapacityBytes / std::max(nodeCount, 1);
            size_t perNodePages = perNodeBytes / config.PageSize;
            nodePageCounts.assign(nodeCount, perNodePages);
        } else if (config.numaConfig->AllocateUsingNodePageSize) {
            size_t pPageSize = Numatic::GetNodePageSize();
            size_t pages = pPageSize / config.PageSize + (pPageSize % config.PageSize ? 1 : 0);
            nodePageCounts.assign(nodeCount, pages);
        } else {
            nodePageCounts.assign(nodeCount, config.InitialPageCount);
        }

        StreamLine::WaitGroup<std::mutex> wg;
        wg.Add(nodeCount);

        for(int i = 0; i < nodeCount; i++){
            size_t nodeNumPages = nodePageCounts[i];
            size_t nodeTotalSize = nodeNumPages * config.PageSize;
            size_t nodeArcCap = nodeTotalSize / sizeof(KeyMeta);

            Logger::getInstance().info(std::string("Node ") + std::to_string(i) + ": " + std::to_string(nodeNumPages) + " pages, " + std::to_string(nodeTotalSize) + " bytes");

            Detail::globalNumaState.Workers[i].Submit([&wg, pNumaState, nodeTotalSize, nodeNumPages, nodeArcCap, &config, i]() {
                void* ptr = Numatic::AllocateLocal(nodeTotalSize);
                if(!ptr){
                    Logger::getInstance().info("NUMA Node page allocator failed.");
                }
                void* auxD = (void*)Numatic::AllocateLocal(sizeof(PerNodeDetails<Policy>));
                Logger::getInstance().info(std::string("Per-node cache will have an inital capacity of ") + std::to_string(nodeArcCap) + " Keys." );
                auto* details = new(auxD) PerNodeDetails<Policy>(nodeArcCap, Numatic::AllocateLocal, Numatic::FreeNUMA);
                if constexpr (Policy::HasRemarcConfig) {
                    details->MaxDeadAge = config.remarcConfig.MaxDeadAge;
                }
                details->PhysicalPageInNode = ptr;
                details->NodePages.reserve(nodeNumPages);
                details->InitReserve(config.ReserveCapacity);
                
                for(size_t pageC = 0; pageC < nodeNumPages; pageC++){
                    details->NodePages.emplace_back(static_cast<char*>(ptr) + (config.PageSize * pageC), config.PageSize, pageC);
                }

                if constexpr (!Policy::HasStoreEviction) {
                    if (nodeNumPages > 1) {
                        details->NodePages[nodeNumPages - 1].Tier.store(PageTier::Cold, std::memory_order_relaxed);
                    }
                }

                if constexpr (Policy::HasStoreEviction) {
                    if (nodeNumPages > 1) {
                        size_t stagingIdx = nodeNumPages - 1;
                        details->NodePages[stagingIdx].Tier.store(PageTier::Staging, std::memory_order_release);
                        details->StagingPageIdx.store(stagingIdx, std::memory_order_release);
                    }
                }
                
                pNumaState->NodeDetails[i] = details;

                wg.Done();
                Logger::getInstance().info("NUMA Node page allocator called WaitGroup::Done().");
            });
        }
        if(!wg.WaitFor(std::chrono::seconds(60))){
            Logger::getInstance().critical("WaitGroup timed out during NUMA page allocation.");
            goto Exit;
        }

        numPages = 0;
        for (int i = 0; i < nodeCount; i++) {
            numPages += nodePageCounts[i];
        }
        totalPhysicalPageSize = numPages * config.PageSize;

    }else{
        if (config.TotalCapacityBytes > 0) {
            numPages = config.TotalCapacityBytes / config.PageSize;
        } else {
            numPages = config.InitialPageCount;
        }
        size_t availMem = MemoryManager::GetAvailableMemory();
        if (availMem < config.PageSize * numPages) {
            while (numPages > 0 && availMem < config.PageSize * numPages) {
                --numPages;
            }
            if (numPages == 0) {
                Logger::getInstance().error("Not enough memory for any pages");
                goto Exit;
            }
        }
    }


    fb = new FurrBall(config, numPages);
    if (!fb) {
        goto Exit;
    }
    
    fb->DataMembers->db = db;
    fb->DataMembers->privateNumaState = pNumaState;
    fb->cache.SetEvictionCallback([fb](const size_t& k, Page*& v)->void { fb->OnEvict(k, v); });

    for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
        auto* details = pNumaState->NodeDetails[i];
        if (details) {
            details->KeyStore.SetEvictionCallback(
                [fb, nodeID = i](const KeyMeta& meta) noexcept {
                    fb->OnKeyEvict(nodeID, meta);
                });
        }
    }

    OpenBalls.push_back(fb);

    if (config.EnableNUMA && Detail::globalNumaState.Workers) {
        for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
            Detail::globalNumaState.Workers[i].StartMaintenance(
                [](int nodeId) {
                    std::vector<FurrBall*> snapshot;
                    {
                        std::lock_guard<std::mutex> lock(OpenBallsMutex);
                        snapshot.assign(OpenBalls.begin(), OpenBalls.end());
                    }
                    for (auto* ball : snapshot) {
                        if (ball->Destroying.load(std::memory_order_acquire)) continue;
                        ball->ActiveMaintenanceRefs.fetch_add(1, std::memory_order_acq_rel);
                        if (ball->Destroying.load(std::memory_order_acquire)) {
                            ball->ActiveMaintenanceRefs.fetch_sub(1, std::memory_order_acq_rel);
                            continue;
                        }
                        ball->BackgroundEvict(nodeId);
                        ball->SyncNodeStats(nodeId);
                        ball->ActiveMaintenanceRefs.fetch_sub(1, std::memory_order_acq_rel);
                    }
                },
                std::chrono::milliseconds(2));
        }
    }
    
    return fb;

Exit:
    if (pNumaState) {
        if (pNumaState->NodeDetails) {
            for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
                auto* details = pNumaState->NodeDetails[i];
                if (details) {
                    if (details->PhysicalPageInNode) {
                        Numatic::FreeNUMA(details->PhysicalPageInNode, totalPhysicalPageSize);
                    }
                    details->~PerNodeDetails<Policy>();
                    Numatic::FreeNUMA(details, sizeof(PerNodeDetails<Policy>));
                }
            }
            free(pNumaState->NodeDetails);
        }
        delete pNumaState;
    }
    if (db) {
        db->Close();
        delete db;
    }
    return nullptr;
}

template<typename Policy>
NuAtlas::FurrBall<Policy>::~FurrBall() noexcept {
    Destroying.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(OpenBallsMutex);
        OpenBalls.remove(this);
    }
    while (ActiveMaintenanceRefs.load(std::memory_order_acquire) > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (DataMembers && DataMembers->db) {
        cache.ForEachValue([this](const size_t&, Page* page) {
            if (page && page->Dirty) {
                FlushPage(page);
            }
        });

        auto status = DataMembers->db->Close();
        if (!status.ok()) {
            Logger::getInstance().error("Failed to close RocksDB: " + status.ToString());
        }
        delete DataMembers->db;
        DataMembers->db = nullptr;

        if (DataMembers->privateNumaState) {
            auto* pns = DataMembers->privateNumaState;
            if (pns->NodeDetails) {
                for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
                    auto* details = pns->NodeDetails[i];
                    if (details) {
                        if (details->PhysicalPageInNode) {
                            size_t physPageSize = details->NodePages.size() * PageSize;
                            Numatic::FreeNUMA(details->PhysicalPageInNode, physPageSize);
                        }
                        details->~PerNodeDetails<Policy>();
                        Numatic::FreeNUMA(details, sizeof(PerNodeDetails<Policy>));
                    }
                }
                free(pns->NodeDetails);
            }
            delete pns;
        }

        delete DataMembers;
    }

    for (void* mem : AllocatedPages) {
        if (UseNUMA) {
            Numatic::FreeNUMA(mem, PageSize);
        } else {
            MemoryManager::FreeMemory(mem);
        }
    }
}

// =====================================================================
//  Explicit instantiations — add new policy types here
// =====================================================================

template class FurrBall<StandardRemarc>;
template class FurrBall<ArcPolicy>;
template class FurrBall<SimpleMigratePolicy>;
template class FurrBall<AugAdaptPolicy>;
template class FurrBall<NativeRemarcPolicy>;
template class FurrBall<LruPolicy>;
