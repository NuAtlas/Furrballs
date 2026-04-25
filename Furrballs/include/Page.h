#pragma once
#include "Remarc.h"
#include "MemoryManager.h"
#include <atomic>
#include <vector>
#include <cassert>

namespace NuAtlas
{
    struct Page {
        void* Data = nullptr;
        size_t PageIndex = 0;
        size_t PageSize = 0;
        std::atomic<size_t> UsedBytes{0};
        std::atomic<size_t> DataWastedByPadding{0};
        bool Dirty = false;

        std::atomic<PageTier> Tier{PageTier::Hot};
        std::atomic<uint16_t> ActiveKeys{0};
        std::vector<uint8_t> TempCtrl;
        std::vector<HashPair> KeyIndex;
        std::vector<uint8_t> HotNodes;
        SpinLock CompactLock;

        Page() noexcept = default;
        Page(void* ptr, size_t pageSize, size_t pageIndex) noexcept
            : Data(ptr), PageIndex(pageIndex), PageSize(pageSize) {};

        Page(const Page&) = delete;
        Page& operator=(const Page&) = delete;

        Page(Page&& other) noexcept
            : Data(other.Data), PageIndex(other.PageIndex), PageSize(other.PageSize), Dirty(other.Dirty),
              TempCtrl(std::move(other.TempCtrl)), KeyIndex(std::move(other.KeyIndex)),
              HotNodes(std::move(other.HotNodes))
        {
            UsedBytes.store(other.UsedBytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
            DataWastedByPadding.store(other.DataWastedByPadding.load(std::memory_order_relaxed), std::memory_order_relaxed);
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
                Tier.store(other.Tier.load(std::memory_order_relaxed), std::memory_order_relaxed);
                ActiveKeys.store(other.ActiveKeys.load(std::memory_order_relaxed), std::memory_order_relaxed);
                TempCtrl = std::move(other.TempCtrl);
                KeyIndex = std::move(other.KeyIndex);
                HotNodes = std::move(other.HotNodes);
                other.Data = nullptr;
            }
            return *this;
        }

        void* TryBump(size_t Size, size_t align = 8) noexcept
        {
            if (Tier.load(std::memory_order_relaxed) != PageTier::Hot) return nullptr;
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

        void AddKeyEntry(const HashPair& hp, uint8_t hotNode = 0) {
            CompactLock.lock();
            KeyIndex.push_back(hp);
            TempCtrl.push_back(PackTempCtrl(REMARC_MAX, 0));
            HotNodes.push_back(hotNode);
            CompactLock.unlock();
            ActiveKeys.fetch_add(1, std::memory_order_relaxed);
        }

        HashPair RemoveKeyEntry(size_t idx) {
            CompactLock.lock();
            assert(idx < KeyIndex.size() && idx < TempCtrl.size() && idx < HotNodes.size());
            size_t last = KeyIndex.size() - 1;
            HashPair swappedHp = KeyIndex[last];
            if (idx != last) {
                KeyIndex[idx] = KeyIndex[last];
                TempCtrl[idx] = TempCtrl[last];
                HotNodes[idx] = HotNodes[last];
            }
            KeyIndex.pop_back();
            TempCtrl.pop_back();
            HotNodes.pop_back();
            size_t newSize = KeyIndex.size();
            CompactLock.unlock();
            ActiveKeys.fetch_sub(1, std::memory_order_relaxed);
            return swappedHp;
        }

        void UpdateTempCtrl(size_t idx, uint8_t tc) noexcept {
            if (idx < TempCtrl.size()) {
                TempCtrl[idx] = tc;
            }
        }

        void UpdateHotNode(size_t idx, uint8_t hn) noexcept {
            if (idx < HotNodes.size()) {
                HotNodes[idx] = hn;
            }
        }

        size_t FindKeyIndex(const HashPair& hp) const noexcept {
            for (size_t i = 0; i < KeyIndex.size(); i++) {
                if (KeyIndex[i].h2 == hp.h2) return i;
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
            KeyIndex.clear();
            TempCtrl.clear();
            HotNodes.clear();
            CompactLock.unlock();
            ActiveKeys.store(0, std::memory_order_relaxed);
            ResetBump();
            Tier.store(PageTier::Empty, std::memory_order_release);
        }

        void ResetBump() noexcept {
            UsedBytes.store(0, std::memory_order_relaxed);
            DataWastedByPadding.store(0, std::memory_order_relaxed);
            Dirty = false;
        }
    };
}
