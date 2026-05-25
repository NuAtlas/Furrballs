#pragma once
#include "CMap.h"
#include <vector>
#include <algorithm>

namespace NuAtlas {

    template<typename Value>
    requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class NativeRemarcStore {
    public:
        using EvictionCallback = std::function<void(const Value&)>;

    private:
        struct Entry {
            std::atomic<uint64_t> h2;
            std::atomic<uint8_t>  desire;
            std::atomic<uint8_t>  freq;
            std::atomic<uint16_t> score;
            uint32_t heapPos;
            bool     occupied;

            Entry& operator=(const Entry& o) {
                h2.store(o.h2.load(std::memory_order_relaxed), std::memory_order_relaxed);
                desire.store(o.desire.load(std::memory_order_relaxed), std::memory_order_relaxed);
                freq.store(o.freq.load(std::memory_order_relaxed), std::memory_order_relaxed);
                score.store(o.score.load(std::memory_order_relaxed), std::memory_order_relaxed);
                heapPos = o.heapPos;
                occupied = o.occupied;
                return *this;
            }
        };

        struct HeapEntry {
            uint16_t score;
            uint32_t slotIdx;
        };

        CMap<Value> store_;
        mutable SpinLock lock_;
        CMapFreeFn freeFn_ = CMapDefaultFree;
        EvictionCallback evictionCallback_ = [](const Value&) {};

        Entry*     slots_    = nullptr;
        size_t     mask_     = 0;
        HeapEntry* heap_     = nullptr;
        uint32_t   heapSize_ = 0;
        uint32_t   heapCap_  = 0;
        uint32_t   touchCount_ = 0;
        uint32_t   rebuildThreshold_ = 0;

        static constexpr uint64_t kEmptyH2  = ~0ULL;
        static constexpr uint32_t kNoHeap    = ~0u;
        static constexpr uint8_t  DESIRE_MAX = 15;
        static constexpr uint8_t  FREQ_MAX   = 31;

        static size_t nextPow2(size_t n) {
            size_t p = 1;
            while (p < n) p <<= 1;
            return p;
        }

        static uint16_t makeScore(uint8_t d, uint8_t f) noexcept {
            uint8_t fd = std::min<uint8_t>(d, 15);
            uint8_t ff = std::min<uint8_t>(f >> 1, 15);
            return (static_cast<uint16_t>(fd) << 4) | static_cast<uint16_t>(ff);
        }

        size_t probe(uint64_t h2) const noexcept {
            size_t idx = h2 & mask_;
            for (size_t i = 0; i <= mask_; ++i) {
                if (!slots_[idx].occupied) return idx;
                if (slots_[idx].h2.load(std::memory_order_relaxed) == h2) return idx;
                idx = (idx + 1) & mask_;
            }
            return SIZE_MAX;
        }

        size_t probeLocked(uint64_t h2) const noexcept {
            size_t idx = h2 & mask_;
            for (size_t i = 0; i <= mask_; ++i) {
                if (!slots_[idx].occupied) return idx;
                if (slots_[idx].h2.load(std::memory_order_acquire) == h2) return idx;
                idx = (idx + 1) & mask_;
            }
            return SIZE_MAX;
        }

        static bool heapCmp(const HeapEntry& a, const HeapEntry& b) noexcept {
            return a.score > b.score;
        }

        uint32_t parent(uint32_t i) const { return (i - 1) / 2; }
        uint32_t left(uint32_t i) const { return 2 * i + 1; }
        uint32_t right(uint32_t i) const { return 2 * i + 2; }

        void siftUp(uint32_t i) {
            while (i > 0) {
                uint32_t p = parent(i);
                if (heapCmp(heap_[p], heap_[i])) break;
                heapSwap(p, i);
                i = p;
            }
        }

        void siftDown(uint32_t i) {
            while (true) {
                uint32_t s = i;
                uint32_t l = left(i), r = right(i);
                if (l < heapSize_ && !heapCmp(heap_[s], heap_[l])) s = l;
                if (r < heapSize_ && !heapCmp(heap_[s], heap_[r])) s = r;
                if (s == i) break;
                heapSwap(s, i);
                i = s;
            }
        }

        void heapSwap(uint32_t a, uint32_t b) {
            HeapEntry tmp = heap_[a];
            heap_[a] = heap_[b];
            heap_[b] = tmp;
            slots_[heap_[a].slotIdx].heapPos = a;
            slots_[heap_[b].slotIdx].heapPos = b;
        }

        void heapPushLocked(uint32_t slotIdx) {
            if (heapSize_ >= heapCap_) return;
            Entry& e = slots_[slotIdx];
            heap_[heapSize_] = {e.score.load(std::memory_order_relaxed), slotIdx};
            e.heapPos = heapSize_;
            siftUp(heapSize_);
            heapSize_++;
        }

        void heapRemoveLocked(uint32_t i) {
            if (i >= heapSize_) return;
            uint32_t lastSlot = heap_[heapSize_ - 1].slotIdx;
            heapSize_--;
            if (i == heapSize_) {
                slots_[lastSlot].heapPos = kNoHeap;
                return;
            }
            heap_[i] = heap_[heapSize_];
            slots_[heap_[i].slotIdx].heapPos = i;
            siftDown(i);
            siftUp(i);
            slots_[lastSlot].heapPos = kNoHeap;
        }

        void heapUpdateScoreLocked(uint32_t slotIdx) {
            Entry& e = slots_[slotIdx];
            uint16_t s = makeScore(
                e.desire.load(std::memory_order_relaxed),
                e.freq.load(std::memory_order_relaxed));
            e.score.store(s, std::memory_order_relaxed);
            if (e.heapPos == kNoHeap) {
                heapPushLocked(slotIdx);
            } else {
                heap_[e.heapPos].score = s;
                siftUp(e.heapPos);
                siftDown(e.heapPos);
            }
        }

        void clearSlot(size_t idx) {
            slots_[idx].h2.store(kEmptyH2, std::memory_order_relaxed);
            slots_[idx].desire.store(0, std::memory_order_relaxed);
            slots_[idx].freq.store(0, std::memory_order_relaxed);
            slots_[idx].score.store(0, std::memory_order_relaxed);
            slots_[idx].heapPos = kNoHeap;
            slots_[idx].occupied = false;
        }

        void eraseEntryLocked(uint64_t h2) noexcept {
            size_t idx = probeLocked(h2);
            if (idx == SIZE_MAX || !slots_[idx].occupied) return;
            if (slots_[idx].heapPos != kNoHeap) heapRemoveLocked(slots_[idx].heapPos);
            clearSlot(idx);
            size_t gap = idx;
            for (size_t i = 1; i <= mask_; ++i) {
                size_t next = (gap + i) & mask_;
                if (!slots_[next].occupied) break;
                size_t home = slots_[next].h2.load(std::memory_order_relaxed) & mask_;
                if ((next >= home && home > gap) ||
                    (home > gap && next < gap) ||
                    (gap > next && next >= home)) {
                    slots_[gap] = slots_[next];
                    clearSlot(next);
                    gap = next;
                }
            }
        }

        void touchKeyLocked(uint64_t h2, uint8_t desireBoost) {
            size_t idx = probeLocked(h2);
            if (idx == SIZE_MAX) return;
            if (!slots_[idx].occupied) {
                slots_[idx].h2.store(h2, std::memory_order_relaxed);
                slots_[idx].desire.store(desireBoost, std::memory_order_relaxed);
                slots_[idx].freq.store(1, std::memory_order_relaxed);
                uint16_t s = makeScore(desireBoost, 1);
                slots_[idx].score.store(s, std::memory_order_relaxed);
                slots_[idx].heapPos = kNoHeap;
                slots_[idx].occupied = true;
                heapPushLocked(static_cast<uint32_t>(idx));
            } else {
                uint8_t d = std::min<uint8_t>(DESIRE_MAX,
                    slots_[idx].desire.load(std::memory_order_relaxed) + desireBoost);
                uint8_t f = std::min<uint8_t>(FREQ_MAX,
                    slots_[idx].freq.load(std::memory_order_relaxed) + 1);
                slots_[idx].desire.store(d, std::memory_order_relaxed);
                slots_[idx].freq.store(f, std::memory_order_relaxed);
                heapUpdateScoreLocked(static_cast<uint32_t>(idx));
            }
        }

        bool evictColdestLocked() {
            while (heapSize_ > 0) {
                uint32_t slotIdx = heap_[0].slotIdx;
                uint64_t h2 = slots_[slotIdx].h2.load(std::memory_order_relaxed);
                heapRemoveLocked(0);

                auto result = store_.FindAndEraseByHash({0, h2});
                if (result.err == NO_ERR) {
                    clearSlot(slotIdx);
                    if (result.value) evictionCallback_(*result.value);
                    return true;
                }
            }
            return false;
        }

        void rebuildHeapLocked() {
            heapSize_ = 0;
            for (size_t i = 0; i <= mask_; ++i) {
                if (!slots_[i].occupied) continue;
                uint16_t s = makeScore(
                    slots_[i].desire.load(std::memory_order_relaxed),
                    slots_[i].freq.load(std::memory_order_relaxed));
                slots_[i].score.store(s, std::memory_order_relaxed);
                slots_[i].heapPos = kNoHeap;
                if (heapSize_ < heapCap_) {
                    heap_[heapSize_] = {s, static_cast<uint32_t>(i)};
                    slots_[i].heapPos = heapSize_;
                    heapSize_++;
                }
            }
            std::make_heap(heap_, heap_ + heapSize_, heapCmp);
        }

    public:
        NativeRemarcStore(size_t capacity,
                          CMapAllocFn af = CMapDefaultAlloc,
                          CMapFreeFn ff = CMapDefaultFree)
            : store_(capacity, af, ff), freeFn_(ff)
        {
            size_t ds = nextPow2(capacity * 2);
            mask_ = ds - 1;
            slots_ = static_cast<Entry*>(af(ds * sizeof(Entry)));
            for (size_t i = 0; i < ds; ++i) {
                slots_[i].h2.store(kEmptyH2, std::memory_order_relaxed);
                slots_[i].desire.store(0, std::memory_order_relaxed);
                slots_[i].freq.store(0, std::memory_order_relaxed);
                slots_[i].score.store(0, std::memory_order_relaxed);
                slots_[i].heapPos = kNoHeap;
                slots_[i].occupied = false;
            }

            heapCap_ = static_cast<uint32_t>(capacity / 4);
            heap_ = static_cast<HeapEntry*>(af(heapCap_ * sizeof(HeapEntry)));
            rebuildThreshold_ = static_cast<uint32_t>(capacity / 8);
        }

        ~NativeRemarcStore() {
            if (slots_) { freeFn_(slots_, (mask_ + 1) * sizeof(Entry)); slots_ = nullptr; }
            if (heap_) { freeFn_(heap_, heapCap_ * sizeof(HeapEntry)); heap_ = nullptr; }
        }

        NativeRemarcStore(const NativeRemarcStore&) = delete;
        NativeRemarcStore& operator=(const NativeRemarcStore&) = delete;

        void SetEvictionCallback(EvictionCallback cb) {
            evictionCallback_ = std::move(cb);
        }

        bool ForceEvictOne() {
            std::lock_guard<SpinLock> guard(lock_);
            return evictColdestLocked();
        }

        uint8_t GetDesire(uint64_t h2) const noexcept {
            size_t idx = probe(h2);
            if (idx == SIZE_MAX || !slots_[idx].occupied) return 0;
            return slots_[idx].desire.load(std::memory_order_relaxed);
        }

        void Decay() noexcept {
            std::lock_guard<SpinLock> guard(lock_);
            for (size_t i = 0; i <= mask_; ++i) {
                if (!slots_[i].occupied) continue;
                uint8_t d = slots_[i].desire.load(std::memory_order_relaxed);
                uint8_t f = slots_[i].freq.load(std::memory_order_relaxed);
                slots_[i].desire.store(d / 2, std::memory_order_relaxed);
                slots_[i].freq.store(std::min<uint8_t>(FREQ_MAX, f + f / 8), std::memory_order_relaxed);
            }
            rebuildHeapLocked();
            touchCount_ = 0;
        }

        std::optional<Value> Find(std::string_view key) {
            auto val = store_.Find(key);
            if (!val) return std::nullopt;

            HashPair hashes = HashKey(key);
            size_t idx = probe(hashes.h2);
            if (idx == SIZE_MAX || !slots_[idx].occupied) return val;

            uint8_t oldD = slots_[idx].desire.load(std::memory_order_relaxed);
            uint8_t newD = std::min<uint8_t>(DESIRE_MAX, oldD + 2);
            slots_[idx].desire.store(newD, std::memory_order_relaxed);

            uint8_t oldF = slots_[idx].freq.load(std::memory_order_relaxed);
            uint8_t newF = std::min<uint8_t>(FREQ_MAX, oldF + 1);
            slots_[idx].freq.store(newF, std::memory_order_relaxed);

            if (++touchCount_ >= rebuildThreshold_) {
                std::lock_guard<SpinLock> guard(lock_);
                if (touchCount_ >= rebuildThreshold_) {
                    rebuildHeapLocked();
                    touchCount_ = 0;
                }
            }
            return val;
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            std::lock_guard<SpinLock> guard(lock_);
            HashPair hashes = HashKey(key);
            eraseEntryLocked(hashes.h2);
            return store_.FindAndErase(key);
        }

        typename CMap<Value>::FindAndEraseResult EraseByHash(const HashPair& hashes) {
            std::lock_guard<SpinLock> guard(lock_);
            eraseEntryLocked(hashes.h2);
            return store_.FindAndEraseByHash(hashes);
        }

        template <typename Fn>
        Error UpdateInPlace(std::string_view key, Fn&& fn) {
            return store_.UpdateInPlace(key, std::forward<Fn>(fn));
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) {
            return store_.UpdateInPlaceByHash(hashes, std::forward<Fn>(fn));
        }

        bool MigrateAndLeaveSentinel(const HashPair&, int) { return false; }
        int FindSentinel(const HashPair&) const { return -1; }

        Error Set(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(lock_);
            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;

            CMapSetResult res = store_.Set(key, val);
            if (res.err == NO_ERR) {
                touchKeyLocked(h2, res.inserted ? 2 : 1);
                return NO_ERR;
            }

            if (!evictColdestLocked()) return res.err;

            res = store_.Set(key, val);
            if (res.err == NO_ERR) {
                touchKeyLocked(h2, 2);
            }
            return res.err;
        }
    };

} // namespace NuAtlas
