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
    void* PhysicalPageInNode = nullptr;
    float EvictThresh = 0.5f;
    size_t PrevMigrationCount = 0;
    size_t PrevDeadKeys = 0;
    float AdaptiveStep = 0.05f;
    uint8_t MaxDeadAge = 8;
    size_t SetFailsSinceMP = 0;
    static constexpr size_t MPTriggerThreshold = 4096ULL;

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
    dstMeta.NodeID = destNode;
    dstMeta.PageIndex = dstPageIdx;
    dstMeta.DataOffset = dstLoc;

    Page& dstPage = dstDetails->NodePages[dstPageIdx];
    uint8_t initTC = Policy::InitialState();
    dstPage.AddKeyEntry(hp, initTC, static_cast<uint8_t>(destNode));
    dstMeta.TempCtrlIdx = static_cast<uint8_t>(dstPage.TempCtrl.size() - 1);
    dstMeta.HotNode = static_cast<uint8_t>(destNode);

    Error insertErr = dstDetails->KeyStore.Set(key, dstMeta);
    if (insertErr != NO_ERR) {
        dstPage.RemoveKeyEntry(dstPage.TempCtrl.size() - 1);
        return false;
    }

    auto erased = srcDetails->KeyStore.Erase(key);
    if (erased.err != NO_ERR) {
        dstDetails->KeyStore.Erase(key);
        dstPage.RemoveKeyEntry(dstPage.TempCtrl.size() - 1);
        return false;
    }

    if (srcMeta->PageIndex < srcDetails->NodePages.size()) {
        Page& srcPage = srcDetails->NodePages[srcMeta->PageIndex];
        size_t idx = srcPage.FindKeyIndex(hp);
        if (idx != SIZE_MAX) {
            HashPair swappedHp = srcPage.RemoveKeyEntry(idx);
            if (idx < srcPage.KeyIndex.size() && swappedHp.h2 != hp.h2) {
                srcDetails->KeyStore.UpdateInPlaceByHash(swappedHp,
                    [idx](KeyMeta& m) {
                    m.TempCtrlIdx = static_cast<uint8_t>(idx);
                });
            }
        }
    }

    Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
    Stats.BytesWritten.fetch_add(dataSize, std::memory_order_relaxed);
    return true;
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
        auto keys = page.KeyIndex;
        page.CompactLock.unlock();

        for (const auto& hp : keys) {
            details->KeyStore.EraseByHash(hp);
        }

        page.Recycle();
        Stats.EvictionCount.fetch_add(static_cast<unsigned int>(keys.size()),
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
                auto keys = page.KeyIndex;
                page.CompactLock.unlock();

                for (const auto& hp : keys) {
                    details->KeyStore.EraseByHash(hp);
                }
                page.Recycle();
                prevDeadCleaned++;
                result.pagesEvicted++;
                result.keysEvicted += keys.size();
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
                        uint8_t desire = details->ShadowDesire.get(page.KeyIndex[k].h2);
                        if (Policy::EvictScore(desire) > 8) {
                            coldMask |= (1ULL << k);
                        }
                    }
                } else {
                    for (size_t k = 0; k < keyCount && k < 64; k++) {
                        uint8_t d = details->KeyStore.GetDesire(page.KeyIndex[k].h2);
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

                    HashPair coldHp = srcWarmPage.KeyIndex[coldBit];
                    uint8_t coldTc = srcWarmPage.TempCtrl[coldBit];
                    HashPair hotHp = srcColdPage.KeyIndex[hotBit];
                    uint8_t hotTc = srcColdPage.TempCtrl[hotBit];

                    HashPair swappedWarm = srcWarmPage.RemoveKeyEntry(coldBit);
                    if (swappedWarm.h2 != coldHp.h2) {
                        details->KeyStore.UpdateInPlaceByHash(swappedWarm,
                            [idx = coldBit](KeyMeta& m) {
                                m.TempCtrlIdx = static_cast<uint8_t>(idx);
                            });
                    }

                    HashPair swappedCold = srcColdPage.RemoveKeyEntry(hotBit);
                    if (swappedCold.h2 != hotHp.h2) {
                        details->KeyStore.UpdateInPlaceByHash(swappedCold,
                            [idx = hotBit](KeyMeta& m) {
                                m.TempCtrlIdx = static_cast<uint8_t>(idx);
                            });
                    }

                    srcWarmPage.AddKeyEntry(hotHp, hotTc);
                    details->KeyStore.UpdateInPlaceByHash(hotHp,
                        [pi = warmPI.idx, d = details](KeyMeta& m) {
                            m.PageIndex = pi;
                            m.TempCtrlIdx = static_cast<uint8_t>(
                                d->NodePages[pi].KeyIndex.size() - 1);
                        });

                    srcColdPage.AddKeyEntry(coldHp, coldTc);
                    details->KeyStore.UpdateInPlaceByHash(coldHp,
                        [pi = coldPI.idx, d = details](KeyMeta& m) {
                            m.PageIndex = pi;
                            m.TempCtrlIdx = static_cast<uint8_t>(
                                d->NodePages[pi].KeyIndex.size() - 1);
                        });

                    coldPI.keyCount = srcColdPage.KeyIndex.size();
                    warmPI.keyCount = srcWarmPage.KeyIndex.size();
                    migrated++;
                    result.keysMigrated++;
                    break;
                }
            }
            }

            if constexpr (!Policy::HasStoreEviction) {
                float& thresh = details->EvictThresh;
                if (policyConfig.StaticEvictThresh >= 0.0f)
                    thresh = policyConfig.StaticEvictThresh;

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
            for (auto& hp : page.KeyIndex) {
                uint8_t d = details->KeyStore.GetDesire(hp.h2);
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
                            Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
                            Stats.BytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
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
                            for (size_t wi = 0; wi < warmPage.KeyIndex.size(); wi++) {
                                uint8_t sc = 0;
                                if constexpr (Policy::HasPerKeyState) {
                                    uint8_t tc = (wi < warmPage.TempCtrl.size()) ? warmPage.TempCtrl[wi] : 0;
                                    sc = Policy::EvictScore(tc);
                                } else {
                                    sc = details->KeyStore.GetDesire(warmPage.KeyIndex[wi].h2);
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
                            HashPair coldHp = warmPage.KeyIndex[bestIdx];

                            HashPair swappedHot = deadPage.RemoveKeyEntry(hotIdx);
                            if (swappedHot.h2 != hp.h2) {
                                details->KeyStore.UpdateInPlaceByHash(swappedHot,
                                    [hIdx = hotIdx](KeyMeta& m) {
                                        m.TempCtrlIdx = static_cast<uint8_t>(hIdx);
                                    });
                            }

                            HashPair swappedCold = warmPage.RemoveKeyEntry(bestIdx);
                            if (swappedCold.h2 != coldHp.h2) {
                                details->KeyStore.UpdateInPlaceByHash(swappedCold,
                                    [bIdx = bestIdx](KeyMeta& m) {
                                        m.TempCtrlIdx = static_cast<uint8_t>(bIdx);
                                    });
                            }

                            details->KeyStore.EraseByHash(coldHp);
                            if (DataMembers && DataMembers->db) {
                                std::string rkey(reinterpret_cast<const char*>(&coldHp), sizeof(HashPair));
                            }

                            warmPage.AddKeyEntry(hp, hotTc);
                            details->KeyStore.UpdateInPlaceByHash(hp,
                                [pi = static_cast<size_t>(bestPage), d = details](KeyMeta& m) {
                                    m.PageIndex = pi;
                                    m.TempCtrlIdx = static_cast<uint8_t>(
                                        d->NodePages[pi].KeyIndex.size() - 1);
                                    m.PageGeneration = d->NodePages[pi].Generation;
                                });

                            Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
                            Stats.EvictionCount.fetch_add(1, std::memory_order_relaxed);
                        } else {
                            deadPage.Tier.store(PageTier::Hot, std::memory_order_release);
                            deadPage.DeadAge = 0;
                            Stats.MigrationCount.fetch_add(1, std::memory_order_relaxed);
                        }

                        Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
                        Stats.BytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
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
                        Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
                        Stats.BytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
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
                                    Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
                                    Stats.BytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
                                    return NO_ERR;
                                }
                            }
                        }

                        tc = isRemote ? Policy::OnRemoteAccess(tc, policyConfig)
                                      : Policy::OnLocalAccess(tc, policyConfig);
                        page.TempCtrl[meta->TempCtrlIdx] = tc;

                        if constexpr (Policy::HasMigration) {
                            if(isRemote && Policy::ShouldHotNode(tc, policyConfig)) {
                                details->KeyStore.UpdateInPlace(key, [currentNode](KeyMeta& m) {
                                    m.HotNode = static_cast<uint8_t>(currentNode);
                                });
                                if(meta->TempCtrlIdx < page.HotNodes.size()) {
                                    page.HotNodes[meta->TempCtrlIdx] = static_cast<uint8_t>(currentNode);
                                }
                            }
                        }
                    }
                }
            }
            outSize = meta->DataSize;
            if(BufSize < meta->DataSize) return BUF_NOT_LARGE_ENOUGH;
            memcpy(outBuf, meta->DataOffset, meta->DataSize);
            Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
            Stats.BytesRead.fetch_add(meta->DataSize, std::memory_order_relaxed);
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
                    Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
                    Stats.BytesRead.fetch_add(fe.size, std::memory_order_relaxed);
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
            Stats.LocalHitCount.fetch_add(1, std::memory_order_relaxed);
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
            Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
            return NO_ERR;
        }
    }

    size_t pageIdx = details->CurrentPage.load(std::memory_order_relaxed);
    void* Loc = nullptr;
    bool didRotate = false;
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
                                if (pg.KeyIndex.empty()) continue;
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
                                    && !details->NodePages[p].KeyIndex.empty()) {
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
                                auto keys = coldPage.KeyIndex;
                                coldPage.CompactLock.unlock();

                                if (!Volatile && DataMembers && DataMembers->db) {
                                    std::string slabKey = "frozen:" + std::to_string(p);
                                    rocksdb::Slice slabVal(
                                        reinterpret_cast<const char*>(coldPage.Data),
                                        coldPage.GetUsedSize());
                                    DataMembers->db->Put(rocksdb::WriteOptions(), slabKey, slabVal);
                                    Stats.BytesWritten.fetch_add(coldPage.GetUsedSize(), std::memory_order_relaxed);

                                    details->FrozenLock.lock();
                                    for (const auto& khp : keys) {
                                        auto it = details->KeyNames.find(khp.h2);
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
                                        details->FrozenIndex[khp.h2] = std::move(fe);
                                    }
                                    details->FrozenLock.unlock();

                                    Stats.FrozenPagesPersisted.fetch_add(1, std::memory_order_relaxed);
                                }

                                for (const auto& khp : keys) {
                                    details->KeyStore.EraseByHash(khp);
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
        size_t nextIdx = pageIdx + 1;
        if (nextIdx >= details->NodePages.size()) {
            pageIdx = details->NodePages.size();
            continue;
        }
        if (!details->CurrentPage.compare_exchange_weak(pageIdx, nextIdx, std::memory_order_relaxed)) {
            pageIdx = details->CurrentPage.load(std::memory_order_relaxed);
        } else {
            pageIdx = nextIdx;
        }
    }
    memcpy(Loc, data, size);

    Page& page = details->NodePages[pageIdx];
    KeyMeta metadata;
    metadata.DataSize = size;
    metadata.NodeID = targetNode;
    metadata.PageIndex = pageIdx;
    metadata.DataOffset = Loc;
    metadata.PageGeneration = page.Generation;

    HashPair hp = HashKey(key);
    uint8_t initTC = Policy::InitialState();
    page.AddKeyEntry(hp, initTC);
    metadata.TempCtrlIdx = static_cast<uint8_t>(page.TempCtrl.size() - 1);

    Error err = details->KeyStore.Set(key, metadata);
    if (err == NO_ERR) {
        Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
        details->KeyNames[hp.h2] = key;
        details->FrozenLock.lock();
        details->FrozenIndex.erase(hp.h2);
        details->FrozenLock.unlock();
        if constexpr (Policy::HasDesire && Policy::HasPerKeyState) {
            uint8_t desire = details->ShadowDesire.get(hp.h2);
            desire = Policy::OnLocalAccess(desire, policyConfig);
            details->ShadowDesire.set(hp.h2, desire);
        }
    } else {
        page.RemoveKeyEntry(page.TempCtrl.size() - 1);
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

    auto& gs = Detail::globalNumaState;
    if (!gs.Initialized) return;
    gs.Initialized = false;

    for(int i = 0; i < gs.NumaNodeCount; i++){
        gs.Workers[i].~NodeJob();
    }
    free(gs.Workers);
    gs.Workers = nullptr;
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

    rocksdb::Status status = rocksdb::DB::Open(options, DBpath, &db);
    if (!status.ok()) {
        Logger::getInstance().error("Failed to open RocksDB at " + DBpath + ": " + status.ToString());
        return nullptr;
    }

    numPages = config.InitialPageCount;
    totalPhysicalPageSize = numPages * config.PageSize;

    if(config.EnableNUMA == true && Detail::globalNumaState.NumaNodeCount){
        if(!config.numaConfig){
            goto Exit;
        }
        pNumaState = new PrivateNumaState<Policy>();
        pNumaState->NodeDetails = (PerNodeDetails<Policy>**)malloc(sizeof(PerNodeDetails<Policy>*) * Detail::globalNumaState.NumaNodeCount);
        pNumaState->rr.SetN(Detail::globalNumaState.NumaNodeCount);
        pNumaState->AllowNodeFallback = config.numaConfig->AllowNodeFallback;
        if(config.numaConfig->AllocateUsingNodePageSize){
            size_t pPageSize = Numatic::GetNodePageSize();
            Logger::getInstance().info(std::string("NUMA node Page size: ") + std::to_string(pPageSize));

            numPages = pPageSize / config.PageSize + (pPageSize % config.PageSize ? 1 : 0);
            totalPhysicalPageSize = numPages * config.PageSize;
        }
        
        Logger::getInstance().info(std::string("Furrballs will allocate a ") + std::to_string(totalPhysicalPageSize) + " bytes sized Physical Page." );
        
        StreamLine::WaitGroup<std::mutex> wg;
        wg.Add(Detail::globalNumaState.NumaNodeCount);
        size_t arcCap = totalPhysicalPageSize / sizeof(KeyMeta);

        for(int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++){
            Detail::globalNumaState.Workers[i].Submit([&wg, pNumaState, totalPhysicalPageSize, numPages, &config, arcCap, i]() {
                void* ptr = Numatic::AllocateLocal(totalPhysicalPageSize);
                if(!ptr){
                    Logger::getInstance().info("NUMA Node page allocator failed.");
                }
                void* auxD = (void*)Numatic::AllocateLocal(sizeof(PerNodeDetails<Policy>));
                Logger::getInstance().info(std::string("Per-node cache will have an inital capacity of ") + std::to_string(arcCap) + " Keys." );
                auto* details = new(auxD) PerNodeDetails<Policy>(arcCap, Numatic::AllocateLocal, Numatic::FreeNUMA);
                if constexpr (Policy::HasRemarcConfig) {
                    details->MaxDeadAge = config.remarcConfig.MaxDeadAge;
                }
                details->PhysicalPageInNode = ptr;
                details->NodePages.reserve(numPages);
                
                for(int pageC = 0; pageC < numPages; pageC++){
                    details->NodePages.emplace_back(static_cast<char*>(ptr) + (config.PageSize * pageC), config.PageSize, pageC);
                }

                if constexpr (!Policy::HasStoreEviction) {
                    if (numPages > 1) {
                        details->NodePages[numPages - 1].Tier.store(PageTier::Cold, std::memory_order_relaxed);
                    }
                }
                
                pNumaState->NodeDetails[i] = details;
                
                wg.Done();
                Logger::getInstance().info("NUMA Node page allocator called WaitGroup::Done().");
            });
        }
        if(!wg.WaitFor(std::chrono::seconds(8))){
            Logger::getInstance().critical("WaitGroup timed out during NUMA page allocation.");
            goto Exit;
        }

    }else{
        numPages = config.InitialPageCount;
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
    
    OpenBalls.push_back(fb);
    
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

        if (DataMembers->privateNumaState) {
            auto* pns = DataMembers->privateNumaState;
            if (pns->NodeDetails) {
                for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
                    auto* details = pns->NodeDetails[i];
                    if (details) {
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

    OpenBalls.remove(this);
}

// =====================================================================
//  Explicit instantiations — add new policy types here
// =====================================================================

template class FurrBall<StandardRemarc>;
template class FurrBall<ArcPolicy>;
template class FurrBall<AugAdaptPolicy>;
template class FurrBall<NativeRemarcPolicy>;
