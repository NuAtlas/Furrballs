#pragma once
#include "Remarc.h"
#include "MemoryManager.h"
#include <atomic>
#include <vector>
#include <cassert>
#include <cstdint>

namespace NuAtlas
{
    struct Page {
        static constexpr uint8_t GROUP_SIZE = 16;
        static constexpr uint8_t MAX_GROUPS = 16; // 256 keys max

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

        uint16_t GroupMask = 0; // bit i = group i has at least one live key
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
              TempCtrl(std::move(other.TempCtrl)), KeyIndex(std::move(other.KeyIndex)),
              HotNodes(std::move(other.HotNodes)), GroupMask(other.GroupMask)
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
                GroupMask = other.GroupMask;
                other.Data = nullptr;
            }
            return *this;
        }

        void* TryBump(size_t Size, size_t align = 8) noexcept
        {
            if (Tier.load(std::memory_order_relaxed) != PageTier::Hot) return nullptr;            size_t oldUsed = UsedBytes.load(std::memory_order_relaxed);
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

        void AddKeyEntry(const HashPair& hp, uint8_t initialTC = PackTempCtrl(REMARC_MAX, 0), uint8_t hotNode = 0) {
            CompactLock.lock();
            size_t idx = KeyIndex.size();
            if (idx >= MAX_GROUPS * GROUP_SIZE) {
                CompactLock.unlock();
                return;
            }
            KeyIndex.push_back(hp);
            TempCtrl.push_back(initialTC);
            HotNodes.push_back(hotNode);
            uint8_t grp = static_cast<uint8_t>(idx / GROUP_SIZE);
            GroupMask |= (1u << grp);
            CompactLock.unlock();
            ActiveKeys.fetch_add(1, std::memory_order_relaxed);
        }

        uint16_t GroupValidMask(uint8_t grp) const noexcept {
            if (grp >= MAX_GROUPS) return 0;
            size_t base = static_cast<size_t>(grp) * GROUP_SIZE;
            size_t end = base + GROUP_SIZE;
            if (end > KeyIndex.size()) end = KeyIndex.size();
            uint16_t mask = 0;
            for (size_t i = base; i < end; i++) {
                mask |= (1u << (i - base));
            }
            return mask;
        }

        uint16_t GetGlobalValidMask() const noexcept {
            uint16_t mask = 0;
            size_t n = KeyIndex.size();
            size_t fullGroups = n / GROUP_SIZE;
            for (size_t g = 0; g < fullGroups; g++) {
                mask |= (0xFFFFu << (g * GROUP_SIZE));
            }
            size_t rem = n % GROUP_SIZE;
            for (size_t i = 0; i < rem; i++) {
                mask |= (1u << (fullGroups * GROUP_SIZE + i));
            }
            return mask;
        }

        void DeleteGroup(uint8_t grp) noexcept {
            CompactLock.lock();
            size_t base = static_cast<size_t>(grp) * GROUP_SIZE;
            size_t end = base + GROUP_SIZE;
            if (end > KeyIndex.size()) end = KeyIndex.size();
            size_t dead = 0;
            for (size_t i = base; i < end; i++) {
                KeyIndex[i].h2 = 0;
                KeyIndex[i].h1 = 0;
                TempCtrl[i] = 0;
                HotNodes[i] = 0;
                dead++;
            }
            CompactLock.unlock();
            if (dead > 0) {
                ActiveKeys.fetch_sub(dead, std::memory_order_relaxed);
            }
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
            CompactLock.unlock();

            uint8_t oldGroup = static_cast<uint8_t>(last / GROUP_SIZE);
            uint8_t newGroup = static_cast<uint8_t>((KeyIndex.size()) / GROUP_SIZE);
            if (oldGroup != newGroup) {
                uint8_t cntOld = static_cast<uint8_t>((oldGroup + 1) * GROUP_SIZE);
                if (KeyIndex.size() < cntOld) cntOld = static_cast<uint8_t>(KeyIndex.size());
                uint8_t cntNew = static_cast<uint8_t>(newGroup * GROUP_SIZE);
                if (KeyIndex.size() > cntNew) cntNew = 0;
                bool oldEmpty = true;
                for (size_t i = static_cast<size_t>(oldGroup) * GROUP_SIZE; i < cntOld; i++) {
                    if (KeyIndex[i].h2 != 0) { oldEmpty = false; break; }
                }
                bool newEmpty = true;
                for (size_t i = cntNew; i < static_cast<size_t>(newGroup + 1) * GROUP_SIZE; i++) {
                    if (i < KeyIndex.size() && KeyIndex[i].h2 != 0) { newEmpty = false; break; }
                }
                if (oldEmpty && !(GroupMask & (1u << oldGroup))) GroupMask &= ~(1u << oldGroup);
                else if (!oldEmpty) GroupMask |= (1u << oldGroup);
                if (newEmpty) GroupMask &= ~(1u << newGroup);
                else GroupMask |= (1u << newGroup);
            }
            ActiveKeys.fetch_sub(1, std::memory_order_relaxed);
            return swappedHp;
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

        size_t GroupCount() const noexcept {
            return (KeyIndex.size() + GROUP_SIZE - 1) / GROUP_SIZE;
        }

        bool IsGroupEmpty(uint8_t grp) const noexcept {
            if (grp >= MAX_GROUPS) return true;
            size_t base = static_cast<size_t>(grp) * GROUP_SIZE;
            size_t end = base + GROUP_SIZE;
            if (end > KeyIndex.size()) end = KeyIndex.size();
            for (size_t i = base; i < end; i++) {
                if (KeyIndex[i].h2 != 0) return false;
            }
            return true;
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
            GroupMask = 0;
            DeadAge = 0;
            CompactLock.unlock();
            ActiveKeys.store(0, std::memory_order_relaxed);
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
