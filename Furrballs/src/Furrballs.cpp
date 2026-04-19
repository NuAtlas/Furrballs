/*****************************************************************//**
 * \file   Furrballs.cpp
 *
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/

#include "Furrballs.h"
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

FurrBall::GlobalNumaState FurrBall::globalNumaState;

struct PerNodeDetails{
    std::shared_mutex rwMutex;
    Numatic::NumaLocalMemoryResource nodeMR;
    std::pmr::vector<Page> NodePages;
    using KeyLock = StreamLine::SeqLock<FurrBall::KeyMeta>;
    std::unordered_map<std::string, std::unique_ptr<KeyLock>> KeyShard;
    size_t CurrentPage = 0;
    void* PhysicalPageInNode = nullptr;
    ARCPolicy<std::string, FurrBall::KeyMeta> KeyMetaStore;
    PerNodeDetails(size_t ARC_cap) : NodePages(&nodeMR), KeyMetaStore(ARC_cap) {}
};

struct PrivateNumaState{
    PerNodeDetails** NodeDetails;
    AtomicRoundRobin rr;
    bool AllowNodeFallback = false;
};

struct FurrBall::ImplDetail {
    rocksdb::DB* db = nullptr;
    PrivateNumaState* privateNumaState = nullptr;
};

NuAtlas::FurrBall::FurrBall(const FurrConfig& config, size_t numPages) noexcept
    : cache(numPages),
    PageSize(config.PageSize),
    MaxPages(numPages),
    Volatile(config.IsVolatile),
    UseNUMA(config.EnableNUMA),
    ThreadLocalRoute(config.numaConfig ? config.numaConfig->UseThreadLocalRouting : false),
    DataMembers(new ImplDetail())
{
}

Page* NuAtlas::FurrBall::AllocatePage(size_t pageIndex) noexcept {
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

void NuAtlas::FurrBall::FlushPage(Page* page) noexcept {
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

void NuAtlas::FurrBall::OnEvict(const size_t& key, Page*& page) noexcept {
    if (!page) return;

    Stats.EvictionCount++;
    if (page->Dirty) {
        FlushPage(page);
    }
}

void* NuAtlas::FurrBall::Get(void* vAddress) noexcept {
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

bool NuAtlas::FurrBall::Set(void* data, size_t size, size_t vAddress) noexcept {
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

Error NuAtlas::FurrBall::Get(const std::string &key, void* outBuf, size_t BufSize, size_t& outSize) noexcept
{
    if(key.empty()){
        Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
        return INVALID_ARG;
    }

    int nodeCount = globalNumaState.NumaNodeCount;
    auto tryShard = [&](int n) -> Error {
        PerNodeDetails* details = DataMembers->privateNumaState->NodeDetails[n];
        auto it = details->KeyShard.find(key);
        if(it != details->KeyShard.end()){
#ifdef SIMULATE_NUMA_LATENCY_NS
            if(Numatic::GetCurrentNode() != n) {
                simulateCrossNodeLatency();
            }
#endif
            KeyMeta meta = it->second->Read();
            outSize = meta.DataSize;
            if(BufSize < meta.DataSize) return BUF_NOT_LARGE_ENOUGH;
            memcpy(outBuf, meta.DataOffset, meta.DataSize);
            Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
            Stats.BytesRead.fetch_add(meta.DataSize, std::memory_order_relaxed);
            return NO_ERR;
        }
        return INVALID_ARG;
    };

    if(ThreadLocalRoute){
        int local = Numatic::GetCurrentNode();
        Error err = tryShard(local);
        if(err == NO_ERR){
            Stats.LocalHitCount.fetch_add(1, std::memory_order_relaxed);
            return NO_ERR;
        }
        for(int n = 0; n < nodeCount; n++){
            if(n == local) continue;
            err = tryShard(n);
            if(err == NO_ERR) return NO_ERR;
        }
    }else{
        for(int n = 0; n < nodeCount; n++){
            Error err = tryShard(n);
            if(err == NO_ERR) return NO_ERR;
        }
    }

    Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
    return INVALID_ARG;
}

Error NuAtlas::FurrBall::Set(const std::string &key, void *data, size_t size) noexcept
{
    if(!data || !size){
        Logger::getInstance().error("Furrball::Set was called with invalid arguments");
        return INVALID_ARG;
    }

    int targetNode = ThreadLocalRoute ? Numatic::GetCurrentNode() : this->DataMembers->privateNumaState->rr.Get();
    PerNodeDetails* details = DataMembers->privateNumaState->NodeDetails[targetNode];
    std::unique_lock<std::shared_mutex> lock(details->rwMutex);

    auto it = details->KeyShard.find(key);

    if(it == details->KeyShard.end()){
        KeyMeta metadata;
        metadata.DataSize = size;
        metadata.NodeID = targetNode;

        void* Loc = details->NodePages[details->CurrentPage].TryBump(size);
        while(Loc == nullptr){
            if(++details->CurrentPage == details->NodePages.size()){
                return OUT_OF_MEM;
            }
            Loc = details->NodePages[details->CurrentPage].TryBump(size);
        }
        memcpy(Loc, data, size);
        metadata.PageIndex = details->CurrentPage;
        metadata.DataOffset = Loc;
        details->KeyShard.insert({key, std::make_unique<PerNodeDetails::KeyLock>(metadata)});
        details->KeyMetaStore.Add(key, metadata);
        Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
    }else{
        KeyMeta meta = it->second->Read();
        if(size <= meta.DataSize){
            memcpy(meta.DataOffset, data, size);
            meta.DataSize = size;
            it->second->Write(meta);
        }else{
            void* Loc = details->NodePages[details->CurrentPage].TryBump(size);
            while(Loc == nullptr){
                if(++details->CurrentPage == details->NodePages.size()){
                    return OUT_OF_MEM;
                }
                Loc = details->NodePages[details->CurrentPage].TryBump(size);
            }
            memcpy(Loc, data, size);
            meta.PageIndex = details->CurrentPage;
            meta.DataOffset = Loc;
            meta.DataSize = size;
            it->second->Write(meta);
            Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
        }
    }
    return NO_ERR;
}

void NuAtlas::FurrBall::Bootstrap()
{
    if (Numatic::IsNUMAAvailable()) {
        globalNumaState.NumaNodeCount = Numatic::GetNodeCount();
        globalNumaState.SysNumaPageSize = Numatic::GetNodePageSize();
        globalNumaState.Workers = (NodeJob*)malloc(sizeof(NodeJob) * globalNumaState.NumaNodeCount);
        Logger::getInstance().info("Creating Node Workers");
        for (int i = 0; i < globalNumaState.NumaNodeCount; i++) {
            new(&globalNumaState.Workers[i]) NodeJob(i);
            globalNumaState.Workers[i].Start([=](){
                Logger::getInstance().info(std::string("Create a node worker pinned on NumaID: ") + std::to_string(i));
            });
        }
    }
}

void NuAtlas::FurrBall::Shutdown()
{
    for (auto &&fb : OpenBalls)
    {
        delete fb;
    }
    for(int i = 0; i < globalNumaState.NumaNodeCount; i++){
        globalNumaState.Workers[i].~NodeJob();
    }
    free(globalNumaState.Workers);
}

FurrBall *FurrBall::CreateBall(const std::string &DBpath, const FurrConfig &config, bool overwrite) noexcept
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

    if(config.EnableNUMA == true && globalNumaState.NumaNodeCount){
        if(!config.numaConfig){
            goto Exit;
        }
        pNumaState = new PrivateNumaState();
        pNumaState->NodeDetails = (PerNodeDetails**)malloc(sizeof(PerNodeDetails*) * globalNumaState.NumaNodeCount);
        pNumaState->rr.SetN(globalNumaState.NumaNodeCount);
        pNumaState->AllowNodeFallback = config.numaConfig->AllowNodeFallback;
        if(config.numaConfig->AllocateUsingNodePageSize){
            size_t pPageSize = Numatic::GetNodePageSize();
            Logger::getInstance().info(std::string("NUMA node Page size: ") + std::to_string(pPageSize));

            numPages = pPageSize / config.PageSize + (pPageSize % config.PageSize ? 1 : 0);
            totalPhysicalPageSize = numPages * config.PageSize;
        }
        
        Logger::getInstance().info(std::string("Furrballs will allocate a ") + std::to_string(totalPhysicalPageSize) + " bytes sized Physical Page." );
        
        StreamLine::WaitGroup<std::mutex> wg;
        wg.Add(globalNumaState.NumaNodeCount);
        size_t arcCap = totalPhysicalPageSize / sizeof(KeyMeta);

        for(int i = 0; i < globalNumaState.NumaNodeCount; i++){
            globalNumaState.Workers[i].Submit([&wg, pNumaState, totalPhysicalPageSize, numPages, &config, arcCap, i]() {
                void* ptr = Numatic::AllocateLocal(totalPhysicalPageSize);
                if(!ptr){
                    Logger::getInstance().info("NUMA Node page allocator failed.");
                }
                void* auxD = (void*)Numatic::AllocateLocal(sizeof(PerNodeDetails));
                Logger::getInstance().info(std::string("Per-node cache will have an inital capacity of ") + std::to_string(arcCap) + " Keys." );
                PerNodeDetails* details = new(auxD)PerNodeDetails(arcCap);
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
            for (int i = 0; i < globalNumaState.NumaNodeCount; i++) {
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

NuAtlas::FurrBall::~FurrBall() noexcept {
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
                for (int i = 0; i < globalNumaState.NumaNodeCount; i++) {
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
