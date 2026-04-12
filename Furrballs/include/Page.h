#pragma once
#include <memory>


namespace NuAtlas
{
    struct Page {
        void* Data = nullptr;
        size_t PageIndex = 0;
        size_t PageSize = 0;
        bool Dirty = false;

        Page() noexcept = default;
        Page(void* ptr, size_t pageSize, size_t pageIndex)noexcept
            : Data(ptr), PageIndex(pageIndex), PageSize(pageSize) {}
    };
} // namespace NuAtlas
