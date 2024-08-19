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
#define unlikely(cond) (__builtin_expect(cond, 0))
#define assume(cond) ((void)0)
#endif

using namespace NuAtlas;

struct ::NuAtlas::FurrBall::ImplDetail{
    rocksdb::DB* db = nullptr;
};

NuAtlas::FurrBall::FurrBall(const FurrConfig& config, ARCPolicy<size_t, void*> pageCache) noexcept : PageSize(config.PageSize),
    SizeLimit(config.CapacityLimit ? config.CapacityLimit : 1 * 1024 * 1024 * sizeof(char)), DataMembers(new ImplDetail()),
    cache(pageCache)
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
    assume(HasThreadInit);
    if(unlikely(!HasThreadInit, 0)){
        FurrSlave = std::thread(&FurrBall::SlaveLoop);
        HasThreadInit = true;
    }

    rocksdb::Options options;
    rocksdb::DB* db;
    options.compression = rocksdb::kLZ4Compression;
    //fb.options.OptimizeForPointLookup();
    options.create_if_missing = true;
    options.optimize_filters_for_hits = true;
    rocksdb::Status status =
        rocksdb::DB::Open(options, DBpath, &db);
    if (!status.ok()) {
        //ERR
        return nullptr;
    }
    //Setup Cache.
    size_t numPages = config.InitialPageCount;
    size_t availMem = MemoryManager::GetAvailableMemory();
    if (availMem < config.PageSize * numPages) {
        //We don't have enough memory to allocate all the pages.
        while ((--numPages) || availMem < config.PageSize * numPages);
        if (numPages <= 0) {
            Logger::getInstance().error("Not enough memory");
            return nullptr;
        }
    }
    ARCPolicy<size_t, void*> Cache(numPages);
    //Allocate Slab.
    char* slab = static_cast<char*>(malloc(config.PageSize * numPages));
    if (!slab) {
        Logger::getInstance().warning("Could not allocated memory slab.");
        //Maybe attempt to allocate fragmented slab.
        //for now return nullptr
        return nullptr;
    }
    FurrBall* fb = new FurrBall(config, Cache);
    fb->Stats.PreallocatedSlabSize = config.PageSize * numPages;
    fb->Stats.UsedMemory = config.PageSize * numPages;
    fb->DataMembers->db = db;
    size_t PagePointer = 0;
    for (int i = 0; i < numPages; i++) {
        Cache.Add(PagePointer, slab + PagePointer);
        //Page type depends on config.
        fb->VPageList.push_back(
            std::variant<Page, LockablePage>(config.LockablePages ? 
            LockablePage(slab + PagePointer, config.PageSize, i) : 
            Page(slab + PagePointer, config.PageSize, i)));
    }
    Cache.SetEvictionCallback([&fb](const size_t& k, void*& v)->void {fb->OnEvict(k, v); });

    OpenBalls.push_back(fb);

    return fb;
}

void NuAtlas::FurrBall::StoreLargeData(void* buffer, size_t size)
{

}

NuAtlas::FurrBall::~FurrBall() noexcept
{
    delete DataMembers->db;
}
