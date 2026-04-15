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

using namespace NuAtlas;

thread_local std::unordered_set<void*> MemoryManager::ThreadBuffers;

FurrBall::GlobalNumaState FurrBall::globalNumaState;

//Stubs until i start using it.


struct PerNodeDetails{
    std::shared_mutex rwMutex;
    Numatic::NumaLocalMemoryResource nodeMR;
    std::pmr::vector<Page> NodePages;
    size_t CurrentPage = 0;
    void* PhysicalPageInNode = nullptr;

    PerNodeDetails() : NodePages(&nodeMR) {}
};

struct PrivateNumaState{
    PerNodeDetails** NodeDetails;
    AtomicRoundRobin rr;
};

struct FurrBall::ImplDetail {
    rocksdb::DB* db = nullptr;
    PrivateNumaState* privateNumaState = nullptr; //< extra state for numa-specific details. I named it private since i there could potentially be a global state.
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
        memory = MemoryManager::AllocateMemory(PageSize);
    }
    if (!memory) {
        return nullptr;
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
    auto iterK = KeyStore.find(key);
    if(key.empty() || iterK == KeyStore.end()){
        Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
        return INVALID_ARG;
    }
    Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
    auto& meta = (*iterK).second;
    outSize = meta.DataSize;
    if(BufSize < meta.DataSize) return BUF_NOT_LARGE_ENOUGH;
    {
        std::unique_lock<std::shared_mutex> lock(DataMembers->privateNumaState->NodeDetails[meta.NodeID]->rwMutex);
        memcpy(outBuf, meta.DataOffset, meta.DataSize);
    }
    Stats.BytesRead.fetch_add(meta.DataSize, std::memory_order_relaxed);
    return NO_ERR;
}

Error NuAtlas::FurrBall::Set(const std::string &key, void *data, size_t size) noexcept
{
    if(!data || !size){
        Logger::getInstance().error("Furrball::Set was called with invalid arguments");
        return INVALID_ARG;
    }
    
    std::unordered_map<std::string, NuAtlas::FurrBall::KeyMeta>::iterator iterK = KeyStore.find(key); //avoiding the const_iterator
    if(iterK == KeyStore.end()){
        //Key not found.
        KeyMeta metadata;
        metadata.DataSize = size;
        metadata.NodeID = ThreadLocalRoute ? Numatic::GetCurrentNode() : this->DataMembers->privateNumaState->rr.Get();
        
        PerNodeDetails* details = this->DataMembers->privateNumaState->NodeDetails[metadata.NodeID];
        std::unique_lock<std::shared_mutex> lock(details->rwMutex);
        void* Loc = details->NodePages[details->CurrentPage].TryBump(size);
        while(Loc == nullptr){
            if(++details->CurrentPage == details->NodePages.size()){
                return OUT_OF_MEM; //all-or-nothing.
            }
            Loc = details->NodePages[details->CurrentPage].TryBump(size);
        }
        memcpy(Loc, data, size);
        metadata.PageIndex = details->CurrentPage;
        metadata.DataOffset = Loc;
        KeyStore.insert({key, metadata});
        Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
    }else{
        /*for future expansion with reclaming and a better allocator*/
        auto& meta = (*iterK).second;
        PerNodeDetails* details = this->DataMembers->privateNumaState->NodeDetails[meta.NodeID];
        if(size <= meta.DataSize){
            //Overwrite it.
            std::unique_lock<std::shared_mutex> lock(details->rwMutex);

            memcpy(meta.DataOffset, data, size);
            meta.DataSize = size;
        }else{ //size > meta.DataSize
            //This wastes spaces, but again simple for phase 1.
            std::unique_lock<std::shared_mutex> lock(details->rwMutex);
            void* Loc = details->NodePages[details->CurrentPage].TryBump(size);
            while(Loc == nullptr){
                if(++details->CurrentPage == details->NodePages.size()){
                    return OUT_OF_MEM; //all-or-nothing.
                }
                Loc = details->NodePages[details->CurrentPage].TryBump(size);
            }
            memcpy(Loc, data, size);
            meta.PageIndex = details->CurrentPage;
            meta.DataOffset = Loc;
            meta.DataSize = size;
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
        //Preallocated buffer.
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
        //Add a general cleanup/exit phase.
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

    size_t numPages = config.InitialPageCount;
    size_t pPageSize;
    size_t totalPhysicalPageSize = numPages * config.PageSize; //Default. Allocates a "slab" divided into two logical pages.

    if(config.EnableNUMA == true && globalNumaState.NumaNodeCount){
        //Allocate Per-ball Numa State.
        pNumaState = new PrivateNumaState();
        pNumaState->NodeDetails = (PerNodeDetails**)malloc(sizeof(PerNodeDetails*) * globalNumaState.NumaNodeCount);
        pNumaState->rr.SetN(globalNumaState.NumaNodeCount);
        //Recalculate Physical Page and page count only if we are using NodePageSize as the Physical Size.
        if(config.numaConfig->AllocateUsingNodePageSize){
            pPageSize = Numatic::GetNodePageSize();
            Logger::getInstance().info(std::string("NUMA node Page size: ") + std::to_string(pPageSize));

            //Calculate Number of Logical subdivions of a NUMA page.
            numPages = pPageSize / config.PageSize + (pPageSize % config.PageSize ? 1 : 0); //If the Page is not an exact fit, we'll allocated one more page.
            totalPhysicalPageSize = numPages * config.PageSize;
        }
        
        Logger::getInstance().info(std::string("Furrballs will allocate a ") + std::to_string(totalPhysicalPageSize) + " bytes sized Physical Page." );
        
        StreamLine::WaitGroup<std::mutex> wg;
        wg.Add(globalNumaState.NumaNodeCount);
        
        for(int i = 0; i < globalNumaState.NumaNodeCount; i++){

            {
                globalNumaState.Workers[i].Submit([&wg, pNumaState, totalPhysicalPageSize, numPages, &config, i]() {
                    void* ptr = Numatic::AllocateLocal(totalPhysicalPageSize);
                    if(!ptr){
                        Logger::getInstance().info("NUMA Node page allocator failed.");
                    }
                    //Allocate Per-node state on the Node itself. This may be premature optimization.
                    void* auxD = (void*)Numatic::AllocateLocal(sizeof(PerNodeDetails));
                    PerNodeDetails* details = new(auxD)PerNodeDetails();

                    details->PhysicalPageInNode = ptr;
                    details->NodePages.reserve(numPages);
                    
                    //Carve Page
                    for(int pageC = 0; pageC < numPages; pageC++){
                        details->NodePages.push_back(Page(static_cast<char*>(ptr) + (config.PageSize * pageC), config.PageSize, pageC));
                    }
                    
                    pNumaState->NodeDetails[i] = details;
                    
                    wg.Done();
                    Logger::getInstance().info("NUMA Node page allocator called WaitGroup::Done().");
                });
            }
        }
        if(!wg.WaitFor(std::chrono::seconds(8))){
            Logger::getInstance().critical("WaitGroup timed out during NUMA page allocation.");
            for (int i = 0; i < globalNumaState.NumaNodeCount; i++) {
                PerNodeDetails* details = pNumaState->NodeDetails[i];
                if (details) {
                    if (details->PhysicalPageInNode) {
                        Numatic::FreeNUMA(details->PhysicalPageInNode, details->PhysicalPageInNode ? totalPhysicalPageSize : 0);
                    }
                    details->~PerNodeDetails();
                    Numatic::FreeNUMA(details, sizeof(PerNodeDetails));
                }
            }
            free(pNumaState->NodeDetails);
            delete pNumaState;
            delete db;
            return nullptr;
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
                delete db;
                return nullptr;
            }
        }
    }


    FurrBall* fb = new FurrBall(config, numPages);
    if (!fb) {
        delete db;
        return nullptr;
    }

    fb->DataMembers->db = db;
    fb->DataMembers->privateNumaState = pNumaState;
    fb->cache.SetEvictionCallback([fb](const size_t& k, Page*& v)->void { fb->OnEvict(k, v); });

    OpenBalls.push_back(fb);

    return fb;
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
