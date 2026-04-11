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

struct FurrBall::ImplDetail {
    rocksdb::DB* db = nullptr;
};

NuAtlas::FurrBall::FurrBall(const FurrConfig& config, size_t numPages) noexcept
    : cache(numPages),
    PageSize(config.PageSize),
    MaxPages(numPages),
    Volatile(config.IsVolatile),
    UseNUMA(config.EnableNUMA),
    DataMembers(new ImplDetail())
{
}

Page* NuAtlas::FurrBall::AllocatePage(size_t pageIndex) noexcept {
    void* memory = nullptr;
    if (UseNUMA) {
#ifndef NO_NUMA
        memory = MemoryManager::AllocateMemoryNUMA(PageSize);
#endif
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

Error NuAtlas::FurrBall::Set(std::string &key, void *data, size_t size) noexcept
{
    if(!data || !size){
        Logger::getInstance().error("Furrball::Set was called with invalid arguments");
        return INVALID_ARG;
    }
    
    return NO_ERR;
}

FurrBall* FurrBall::CreateBall(const std::string& DBpath, const FurrConfig& config, bool overwrite) noexcept {
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;

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

    FurrBall* fb = new FurrBall(config, numPages);
    if (!fb) {
        delete db;
        return nullptr;
    }

    fb->DataMembers->db = db;
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
        delete DataMembers;
    }

    for (void* mem : AllocatedPages) {
        MemoryManager::FreeMemory(mem);
    }

    OpenBalls.remove(this);
}
