/*****************************************************************//**
 * \file   Furrballs.cpp
 * 
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/

#include "Furrballs.h"

using namespace Furrball;

struct FurrBall::ImplDetail {
    rocksdb::DB* db = nullptr;
};

Furrball::FurrBall::FurrBall(std::unique_ptr<ImplDetail> impl)
{
    this->DataMembers.swap(impl);
}

FurrBall* FurrBall::CreateBall(std::string DBpath, size_t PageSize, size_t numPages, bool overwrite) noexcept
{
    rocksdb::Options options;
    rocksdb::DB* db;
    options.compression = rocksdb::kLZ4Compression;
    //fb.options.OptimizeForPointLookup();
    options.create_if_missing = true;
    rocksdb::Status status =
        rocksdb::DB::Open(options, DBpath, &db);
    if (!status.ok()) {
        //ERR
        return nullptr;
    }
    //Setup Cache.
    size_t availMem = MemoryManager::GetAvailableMemory();
    if (availMem < PageSize * numPages) {
        //We don't have enough memory to allocate all the pages.
        while ((--numPages) || availMem < PageSize * numPages);
        if (numPages <= 0) {
            Logger::getInstance().error("Not enough memory");
            return nullptr;
        }
    }
    ARCPolicy<size_t, void*> Cache(numPages);
    //Allocate Slab.
    char* slab = static_cast<char*>(malloc(PageSize * numPages));
    if (!slab) {
        Logger::getInstance().warning("Could not allocated memory slab.");
        //Maybe attempt to allocate fragmented slab.
        //for now return nullptr
        return nullptr;
    }
    size_t PagePointer = 0;
    for (int i = 0; i < numPages; i++) {
        Cache.add(PagePointer, slab + PagePointer);
    }
    FurrBall* fb = new FurrBall();
    fb->DataMembers->db = db;
    return fb;
}

void Furrball::FurrBall::StoreLargeData(void* buffer, size_t size)
{

}

Furrball::FurrBall::~FurrBall() noexcept
{
    delete DataMembers->db;
}
