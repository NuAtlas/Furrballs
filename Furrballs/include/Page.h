#pragma once
#include "MemoryManager.h"
#include <memory>


namespace NuAtlas
{

    //Bump allocator. Next phase, add a strategy template. We have C++20 + Concepts now.
    //Pages are not supposed to be under contention (this struct specifically).
    struct Page {
        void* Data = nullptr;
        size_t PageIndex = 0;
        size_t PageSize = 0;
        size_t UsedBytes = 0; //Just for the bump allocator, this can be used an offset to free memory.
        size_t DataWastedByPadding = 0; //This is for metrics. It's not really useful elsewhere.
        bool Dirty = false;

        Page() noexcept = default;
        Page(void* ptr, size_t pageSize, size_t pageIndex)noexcept
            : Data(ptr), PageIndex(pageIndex), PageSize(pageSize) {};
        
        //Keeping this simple, It will track start position only. 
        void* TryBump(size_t Size, size_t align = 8) noexcept
        {
            size_t head = padded_size_to(UsedBytes, align);  // relative offset, aligned
            if (head + Size <= PageSize) {
                DataWastedByPadding += head - UsedBytes;
                UsedBytes = head + Size;
                return static_cast<char*>(Data) + head;
            }else{
                return nullptr;
            }
        }

        size_t GetUsedSize() const noexcept { return UsedBytes;}
        size_t GetDataSize() const noexcept { return UsedBytes - DataWastedByPadding;}
        size_t GetTotalPaddingBytes() const noexcept { return DataWastedByPadding; }
    };
} // namespace NuAtlas
