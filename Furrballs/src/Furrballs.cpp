/*********************************************************************\
 * \file   Furrballs.cpp
 *
 * \author The Sphynx
 * \date   July 2024
\*********************************************************************/

#include "Furrballs.h"
#include "CMap.h"
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

struct PerNodeDetails{
    Numatic::NumaLocalMemoryResource nodeMR;
    std::pmr::vector<Page> NodePages;
    ConcurrentARC<KeyMeta> KeyStore;
    std::unordered_map<HashPair, uint8_t, decltype([](const HashPair& h) -> size_t {
        return std::rotl(static_cast<size_t>(h.h1), 7) ^ h.h2;
    })> ShadowDesire;
    SpinLock DesireLock;
    std::atomic<uint8_t> MinDesire{0};
    std::atomic<size_t> CurrentPage{0};
    void* PhysicalPageInNode = nullptr;
    PerNodeDetails(size_t arcCap, CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
        : NodePages(&nodeMR), KeyStore(arcCap, af, ff) {}
};

struct PrivateNumaState{
    PerNodeDetails** NodeDetails;
    AtomicRoundRobin rr;
    bool AllowNodeFallback = false;
};

template<typename Policy>
struct FurrBall<Policy>::ImplDetail {
    rocksdb::DB* db = nullptr;
    PrivateNumaState* privateNumaState = nullptr;
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

    PerNodeDetails* srcDetails = DataMembers->privateNumaState->NodeDetails[sourceNode];
    PerNodeDetails* dstDetails = DataMembers->privateNumaState->NodeDetails[destNode];

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

    PerNodeDetails* details = DataMembers->privateNumaState->NodeDetails[nodeID];

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
            if (ePage > static_cast<float>(policyConfig.ThetaEvict)) {
                page.TryTransition(PageTier::Hot, PageTier::Freeze);
            }
        }

        if constexpr (Policy::HasDesire) {
            details->MinDesire.store(maxEvictScore, std::memory_order_relaxed);
        }
    }
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
        PerNodeDetails* details = DataMembers->privateNumaState->NodeDetails[n];
        auto meta = details->KeyStore.Find(key);
        if(meta.has_value()){
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
                            PerNodeDetails* localDetails = DataMembers->privateNumaState->NodeDetails[currentNode];
                            uint8_t desire = localDetails->ShadowDesire.count(hp)
                                ? localDetails->ShadowDesire[hp] : 0;
                            desire = Policy::OnLocalAccess(desire, policyConfig);
                            localDetails->DesireLock.lock();
                            localDetails->ShadowDesire[hp] = desire;
                            localDetails->DesireLock.unlock();

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
    PerNodeDetails* details = DataMembers->privateNumaState->NodeDetails[targetNode];

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
    while (true) {
        if (pageIdx >= details->NodePages.size()) return OUT_OF_MEM;
        Loc = details->NodePages[pageIdx].TryBump(size);
        if (Loc) break;
        size_t nextIdx = pageIdx + 1;
        if (nextIdx >= details->NodePages.size()) return OUT_OF_MEM;
        if (!details->CurrentPage.compare_exchange_weak(pageIdx, nextIdx, std::memory_order_relaxed)) {
            pageIdx = details->CurrentPage.load(std::memory_order_relaxed);
        } else {
            pageIdx = nextIdx;
        }
    }
    memcpy(Loc, data, size);

    KeyMeta metadata;
    metadata.DataSize = size;
    metadata.NodeID = targetNode;
    metadata.PageIndex = pageIdx;
    metadata.DataOffset = Loc;

    Page& page = details->NodePages[pageIdx];
    HashPair hp = HashKey(key);
    uint8_t initTC = Policy::InitialState();
    page.AddKeyEntry(hp, initTC);
    metadata.TempCtrlIdx = static_cast<uint8_t>(page.TempCtrl.size() - 1);

    Error err = details->KeyStore.Set(key, metadata);
    if (err == NO_ERR) {
        Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
        if constexpr (Policy::HasDesire) {
            uint8_t desire = 0;
            details->DesireLock.lock();
            auto it = details->ShadowDesire.find(hp);
            if (it != details->ShadowDesire.end()) desire = it->second;
            desire = Policy::OnLocalAccess(desire, policyConfig);
            details->ShadowDesire[hp] = desire;
            details->DesireLock.unlock();
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
    PrivateNumaState* pNumaState = nullptr;
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
        pNumaState = new PrivateNumaState();
        pNumaState->NodeDetails = (PerNodeDetails**)malloc(sizeof(PerNodeDetails*) * Detail::globalNumaState.NumaNodeCount);
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
                void* auxD = (void*)Numatic::AllocateLocal(sizeof(PerNodeDetails));
                Logger::getInstance().info(std::string("Per-node cache will have an inital capacity of ") + std::to_string(arcCap) + " Keys." );
                PerNodeDetails* details = new(auxD)PerNodeDetails(arcCap, Numatic::AllocateLocal, Numatic::FreeNUMA);
                details->PhysicalPageInNode = ptr;
                details->NodePages.reserve(numPages);
                
                for(int pageC = 0; pageC < numPages; pageC++){
                    details->NodePages.emplace_back(static_cast<char*>(ptr) + (config.PageSize * pageC), config.PageSize, pageC);
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
                PerNodeDetails* details = pNumaState->NodeDetails[i];
                if (details) {
                    if (details->PhysicalPageInNode) {
                        Numatic::FreeNUMA(details->PhysicalPageInNode, totalPhysicalPageSize);
                    }
                    details->~PerNodeDetails();
                    Numatic::FreeNUMA(details, sizeof(PerNodeDetails));
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
            PrivateNumaState* pns = DataMembers->privateNumaState;
            if (pns->NodeDetails) {
                for (int i = 0; i < Detail::globalNumaState.NumaNodeCount; i++) {
                    PerNodeDetails* details = pns->NodeDetails[i];
                    if (details) {
                        details->~PerNodeDetails();
                        Numatic::FreeNUMA(details, sizeof(PerNodeDetails));
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
