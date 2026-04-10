/*****************************************************************//**
 * \file   Furrballs.cpp
 * 
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/

#include "Furrballs.h"
#undef max
#undef min
#include <string_view>
#include <rocksdb/db.h>
#include <rocksdb/advanced_options.h>
#include "MemoryManager.h"

#ifdef _WIN32
#define unlikely(cond) (cond)
#define assume(cond) __assume(cond)
#else
#define unlikely(cond) (__builtin_expect(!!(cond), 0))
#define assume(cond) ((void)0)
#endif

using namespace NuAtlas;

thread_local std::unordered_set<void*> MemoryManager::ThreadBuffers;

std::mutex FurrBall::JobMutex;
std::queue<std::function<void()>> FurrBall::JobQueue;

struct ::NuAtlas::FurrBall::ImplDetail{
    rocksdb::DB* db = nullptr;
};

NuAtlas::FurrBall::FurrBall(const FurrConfig& config, size_t numPages) noexcept : cache(numPages),
    PageSize(config.PageSize),
    SizeLimit(config.CapacityLimit ? config.CapacityLimit : 1 * 1024 * 1024 * sizeof(char)),
    DataMembers(new ImplDetail())
{
}

void NuAtlas::FurrBall::OnEvict(const size_t& key, void*& value) noexcept
{

}

void NuAtlas::FurrBall::SlaveLoop()
{
}

FurrBall* FurrBall::CreateBall(const std::string& DBpath, const FurrConfig& config, bool overwrite) noexcept
{
    if (unlikely(!HasThreadInit)) {
        FurrSlave = std::thread(&FurrBall::SlaveLoop);
        HasThreadInit = true;
    }

    rocksdb::Options options;
    rocksdb::DB* db;
    options.create_if_missing = true;
    options.optimize_filters_for_hits = true;
    rocksdb::Status status =
        rocksdb::DB::Open(options, DBpath, &db);
    if (!status.ok()) {
        return nullptr;
    }
    size_t numPages = config.InitialPageCount;
    size_t availMem = MemoryManager::GetAvailableMemory();
    if (availMem < config.PageSize * numPages) {
        while (numPages > 0 && availMem < config.PageSize * numPages) {
            --numPages;
        }
        if (numPages == 0) {
            Logger::getInstance().error("Not enough memory");
            return nullptr;
        }
    }

    FurrBall* fb = new FurrBall(config, numPages);

    char* slab = static_cast<char*>(MemoryManager::AllocateMemory(config.PageSize * numPages));
    if (!slab) {
        Logger::getInstance().warning("Could not allocate memory slab.");
        delete db;
        delete fb;
        return nullptr;
    }

    fb->Stats.PreallocatedSlabSize = config.PageSize * numPages;
    fb->Stats.UsedMemory = config.PageSize * numPages;
    fb->DataMembers->db = db;

    size_t pagePtr = 0;
    for (size_t i = 0; i < numPages; i++) {
        fb->cache.Add(pagePtr, slab + pagePtr);
        fb->VPageList.push_back(
            config.LockablePages
                ? std::variant<Page, LockablePage>(LockablePage(slab + pagePtr, config.PageSize, i))
                : std::variant<Page, LockablePage>(Page(slab + pagePtr, config.PageSize, i)));
        pagePtr += config.PageSize;
    }
    fb->cache.SetEvictionCallback([fb](const size_t& k, void*& v)->void { fb->OnEvict(k, v); });

    OpenBalls.push_back(fb);

    return fb;
}

void NuAtlas::FurrBall::StoreLargeData(void* buffer, size_t size)
{

}

NuAtlas::FurrBall::~FurrBall() noexcept
{
    if (DataMembers && DataMembers->db) {
        auto status = DataMembers->db->Close();
        if (!status.ok()) {
            Logger::getInstance().error("Failed to close RocksDB: " + status.ToString());
        }
        delete DataMembers;
    }

    OpenBalls.remove(this);
}
