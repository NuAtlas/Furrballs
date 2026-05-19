#pragma once
#include "Remarc.h"
#include "MemoryManager.h"
#include <atomic>
#include <vector>
#include <cassert>
#include <cstdint>

namespace NuAtlas
{
    struct FreeSlot {
        uint32_t offset;
        uint32_t size;
    };

    struct Page {
        void* Data = nullptr;
        size_t PageIndex = 0;
        size_t PageSize = 0;
        std::atomic<size_t> UsedBytes{0};
        std::atomic<size_t> DataWastedByPadding{0};
        std::atomic<size_t> DeadBytes{0};
        bool Dirty = false;

        std::atomic<PageTier> Tier{PageTier::Hot};
        std::atomic<uint16_t> ActiveKeys{0};
        std::vector<uint8_t> TempCtrl;
        std::vector<uint64_t> KeyH1;
        std::vector<uint64_t> KeyH2;
        std::vector<FreeSlot> FreeSlots;

        uint32_t Generation = 0;
        uint8_t DeadAge = 0;
        SpinLock CompactLock;

        Page() noexcept = default;
        Page(void* ptr, size_t pageSize, size_t pageIndex) noexcept
            : Data(ptr), PageIndex(pageIndex), PageSize(pageSize) {};

        Page(const Page&) = delete;
        Page& operator=(const Page&) = delete;

        Page(Page&& other) noexcept
            : Data(other.Data), PageIndex(other.PageIndex), PageSize(other.PageSize), Dirty(other.Dirty),
              TempCtrl(std::move(other.TempCtrl)), KeyH1(std::move(other.KeyH1)),
              KeyH2(std::move(other.KeyH2)), FreeSlots(std::move(other.FreeSlots))
        {
            UsedBytes.store(other.UsedBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
            DataWastedByPadding.store(other.DataWastedByPadding.load(std::memory_order_relaxed), std::memory_order_relaxed);
            DeadBytes.store(other.DeadBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
            Tier.store(other.Tier.load(std::memory_order_relaxed), std::memory_order_relaxed);
            ActiveKeys.store(other.ActiveKeys.load(std::memory_order_relaxed), std::memory_order_relaxed);
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
                DeadBytes.store(other.DeadBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
                Tier.store(other.Tier.load(std::memory_order_relaxed), std::memory_order_relaxed);
                ActiveKeys.store(other.ActiveKeys.load(std::memory_order_relaxed), std::memory_order_relaxed);
                TempCtrl = std::move(other.TempCtrl);
                KeyH1 = std::move(other.KeyH1);
                KeyH2 = std::move(other.KeyH2);
                FreeSlots = std::move(other.FreeSlots);
                other.Data = nullptr;
            }
            return *this;
        }

        void* TryBump(size_t Size, size_t align = 8) noexcept
        {
            auto tier = Tier.load(std::memory_order_relaxed);
            if (tier != PageTier::Hot && tier != PageTier::Staging) return nullptr;            size_t oldUsed = UsedBytes.load(std::memory_order_relaxed);
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
        size_t GetDeadBytes() const noexcept { return DeadBytes.load(std::memory_order_relaxed); }
        size_t GetLiveBytes() const noexcept { return GetDataSize() - DeadBytes.load(std::memory_order_relaxed); }

        void AddFreeSlot(uint32_t offset, uint32_t size) noexcept {
            CompactLock.lock();
            FreeSlots.push_back({offset, size});
            CompactLock.unlock();
            DeadBytes.fetch_add(size, std::memory_order_relaxed);
            Dirty = true;
        }

        void* TryAllocFromFree(size_t size, size_t align = 8) noexcept {
            if (Tier.load(std::memory_order_relaxed) != PageTier::Hot) return nullptr;
            CompactLock.lock();
            for (size_t i = 0; i < FreeSlots.size(); i++) {
                uint32_t origOffset = FreeSlots[i].offset;
                uint32_t origEnd = FreeSlots[i].offset + FreeSlots[i].size;
                uint32_t aligned = static_cast<uint32_t>(padded_size_to(origOffset, align));
                uint32_t end = aligned + static_cast<uint32_t>(size);
                if (end <= origEnd) {
                    void* ptr = static_cast<char*>(Data) + aligned;
                    if (end == origEnd) {
                        FreeSlots.erase(FreeSlots.begin() + static_cast<ptrdiff_t>(i));
                    } else {
                        FreeSlots[i] = {end, origEnd - end};
                    }
                    CompactLock.unlock();
                    DeadBytes.fetch_sub(end - aligned, std::memory_order_relaxed);
                    Dirty = true;
                    return ptr;
                }
            }
            CompactLock.unlock();
            return nullptr;
        }

        void AddKeyEntry(const HashPair& hp, uint8_t initialTC = PackTempCtrl(REMARC_MAX, 0)) {
            CompactLock.lock();
            KeyH1.push_back(hp.h1);
            KeyH2.push_back(hp.h2);
            TempCtrl.push_back(initialTC);
            CompactLock.unlock();
            ActiveKeys.fetch_add(1, std::memory_order_relaxed);
        }

        HashPair RemoveKeyEntry(size_t idx) {
            CompactLock.lock();
            assert(idx < KeyH1.size() && idx < KeyH2.size() && idx < TempCtrl.size());
            size_t last = KeyH2.size() - 1;
            HashPair swapped{KeyH1[last], KeyH2[last]};
            if (idx != last) {
                KeyH1[idx] = KeyH1[last];
                KeyH2[idx] = KeyH2[last];
                TempCtrl[idx] = TempCtrl[last];
            }
            KeyH1.pop_back();
            KeyH2.pop_back();
            TempCtrl.pop_back();
            CompactLock.unlock();
            ActiveKeys.fetch_sub(1, std::memory_order_relaxed);
            return swapped;
        }

        void RemoveKeyByHash(const HashPair& hp) noexcept {
            size_t idx = FindKeyIndex(hp);
            if (idx != SIZE_MAX) RemoveKeyEntry(idx);
        }

        void UpdateTempCtrl(size_t idx, uint8_t tc) noexcept {
            if (idx < TempCtrl.size()) {
                TempCtrl[idx] = tc;
            }
        }

        size_t FindKeyIndex(const HashPair& hp) const noexcept {
            for (size_t i = 0; i < KeyH2.size(); i++) {
                if (KeyH2[i] == hp.h2) return i;
            }
            return SIZE_MAX;
        }

        bool TryTransition(PageTier from, PageTier to) noexcept {
            PageTier expected = from;
            return Tier.compare_exchange_strong(expected, to,
                std::memory_order_acq_rel, std::memory_order_relaxed);
        }

        void Recycle() noexcept {
            CompactLock.lock();
            KeyH1.clear();
            KeyH2.clear();
            TempCtrl.clear();
            FreeSlots.clear();
            DeadAge = 0;
            CompactLock.unlock();
            ActiveKeys.store(0, std::memory_order_relaxed);
            DeadBytes.store(0, std::memory_order_relaxed);
            ResetBump();
            Generation++;
            Tier.store(PageTier::Empty, std::memory_order_release);
        }

        void ResetBump() noexcept {
            UsedBytes.store(0, std::memory_order_relaxed);
            DataWastedByPadding.store(0, std::memory_order_relaxed);
            Dirty = false;
        }
    };
}
