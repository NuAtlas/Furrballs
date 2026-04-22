#pragma once
#include "MemoryManager.h"
#include <atomic>
#include <memory>


namespace NuAtlas
{
    struct Page {
        void* Data = nullptr;
        size_t PageIndex = 0;
        size_t PageSize = 0;
        std::atomic<size_t> UsedBytes{0};
        std::atomic<size_t> DataWastedByPadding{0};
        bool Dirty = false;

        Page() noexcept = default;
        Page(void* ptr, size_t pageSize, size_t pageIndex)noexcept
            : Data(ptr), PageIndex(pageIndex), PageSize(pageSize) {};

        Page(const Page&) = delete;
        Page& operator=(const Page&) = delete;

        Page(Page&& other) noexcept
            : Data(other.Data), PageIndex(other.PageIndex), PageSize(other.PageSize), Dirty(other.Dirty)
        {
            UsedBytes.store(other.UsedBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
            DataWastedByPadding.store(other.DataWastedByPadding.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.Data = nullptr;
        }

        Page& operator=(Page&& other) noexcept {
            if (this != &other) {
                Data = other.Data;
                PageIndex = other.PageIndex;
                PageSize = other.PageSize;
                Dirty = other.Dirty;
                UsedBytes.store(other.UsedBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
                DataWastedByPadding.store(other.DataWastedByPadding.load(std::memory_order_relaxed), std::memory_order_relaxed);
                other.Data = nullptr;
            }
            return *this;
        }

        void* TryBump(size_t Size, size_t align = 8) noexcept
        {
            size_t oldUsed = UsedBytes.load(std::memory_order_relaxed);
            size_t head;
            do {
                head = padded_size_to(oldUsed, align);
                if (head + Size > PageSize) return nullptr;
            } while (!UsedBytes.compare_exchange_weak(oldUsed, head + Size,
                         std::memory_order_acq_rel, std::memory_order_relaxed));
            size_t waste = head - oldUsed;
            if (waste > 0) {
                DataWastedByPadding.fetch_add(waste, std::memory_order_relaxed);
            }
            Dirty = true;
            return static_cast<char*>(Data) + head;
        }

        size_t GetUsedSize() const noexcept { return UsedBytes.load(std::memory_order_relaxed); }
        size_t GetDataSize() const noexcept { return UsedBytes.load(std::memory_order_relaxed) - DataWastedByPadding.load(std::memory_order_relaxed); }
        size_t GetTotalPaddingBytes() const noexcept { return DataWastedByPadding.load(std::memory_order_relaxed); }
    };
}
