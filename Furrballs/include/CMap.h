#pragma once
#include "Error.h"
#include "Remarc.h"
#include <bit>
#include <atomic>
#include <xxh3.h>
#include <xxhash.h>
#include <optional>
#include <cstring>
#include <cassert>
#include <new>
#include <string>
#include <algorithm>
#include <functional>
#include <unordered_map>

namespace NuAtlas {

    template<typename V = uint32_t>
    class OpenIdx {
        static constexpr uint64_t kEmpty = UINT64_MAX;

        std::vector<uint64_t> keys_;
        std::vector<V> vals_;
        size_t mask_;

    public:
        explicit OpenIdx(size_t capacity) {
            size_t sz = 4;
            while (sz < capacity * 3) sz <<= 1;
            keys_.assign(sz, kEmpty);
            vals_.assign(sz, V{});
            mask_ = sz - 1;
        }

        bool contains(uint64_t key) const noexcept {
            size_t idx = key & mask_;
            for (size_t i = 0; i <= mask_; i++) {
                if (keys_[idx] == key) return true;
                if (keys_[idx] == kEmpty) return false;
                idx = (idx + 1) & mask_;
            }
            return false;
        }

        V* find(uint64_t key) noexcept {
            size_t idx = key & mask_;
            for (size_t i = 0; i <= mask_; i++) {
                if (keys_[idx] == key) return &vals_[idx];
                if (keys_[idx] == kEmpty) return nullptr;
                idx = (idx + 1) & mask_;
            }
            return nullptr;
        }

        const V* find(uint64_t key) const noexcept {
            size_t idx = key & mask_;
            for (size_t i = 0; i <= mask_; i++) {
                if (keys_[idx] == key) return &vals_[idx];
                if (keys_[idx] == kEmpty) return nullptr;
                idx = (idx + 1) & mask_;
            }
            return nullptr;
        }

        void insert(uint64_t key, V value) noexcept {
            size_t idx = key & mask_;
            for (;;) {
                if (keys_[idx] == kEmpty || keys_[idx] == key) {
                    keys_[idx] = key;
                    vals_[idx] = value;
                    return;
                }
                idx = (idx + 1) & mask_;
            }
        }

        void erase(uint64_t key) noexcept {
            size_t idx = key & mask_;
            for (size_t i = 0; i <= mask_; i++) {
                if (keys_[idx] == key) {
                    keys_[idx] = kEmpty;
                    size_t gap = idx;
                    size_t cur = (idx + 1) & mask_;
                    while (keys_[cur] != kEmpty) {
                        size_t ideal = keys_[cur] & mask_;
                        size_t probe_dist = (cur - ideal) & mask_;
                        size_t gap_dist = (cur - gap) & mask_;
                        if (gap_dist <= probe_dist) {
                            keys_[gap] = keys_[cur];
                            vals_[gap] = vals_[cur];
                            keys_[cur] = kEmpty;
                            gap = cur;
                        }
                        cur = (cur + 1) & mask_;
                    }
                    return;
                }
                if (keys_[idx] == kEmpty) return;
                idx = (idx + 1) & mask_;
            }
        }
    };

    class FlatList {
        static constexpr uint32_t NIL = UINT32_MAX;

        struct Slot {
            HashPair hashes;
            uint32_t prev;
            uint32_t next;
        };

        std::vector<Slot> slots_;
        OpenIdx<> idx_;
        uint32_t head_{NIL};
        uint32_t tail_{NIL};
        uint32_t freeHead_{NIL};
        uint32_t count_{0};

        uint32_t allocSlot() {
            uint32_t s = freeHead_;
            freeHead_ = slots_[s].next;
            return s;
        }

        void freeSlot(uint32_t s) {
            slots_[s].hashes = {};
            slots_[s].prev = NIL;
            slots_[s].next = freeHead_;
            freeHead_ = s;
        }

    public:
        explicit FlatList(size_t maxEntries)
            : idx_(maxEntries) {
            slots_.resize(maxEntries);
            for (size_t i = 0; i < maxEntries; i++) {
                slots_[i] = {{}, NIL, static_cast<uint32_t>(i + 1)};
            }
            slots_[maxEntries - 1].next = NIL;
            freeHead_ = 0;
        }

        bool contains(uint64_t h2) const noexcept {
            return idx_.contains(h2);
        }

        void push_front(HashPair hashes) {
            uint32_t s = allocSlot();
            slots_[s].hashes = hashes;
            slots_[s].prev = NIL;
            slots_[s].next = head_;
            if (head_ != NIL) slots_[head_].prev = s;
            else tail_ = s;
            head_ = s;
            idx_.insert(hashes.h2, s);
            count_++;
        }

        void pop_back() {
            if (tail_ == NIL) return;
            uint32_t s = tail_;
            idx_.erase(slots_[s].hashes.h2);
            uint32_t p = slots_[s].prev;
            if (p != NIL) slots_[p].next = NIL;
            else head_ = NIL;
            tail_ = p;
            freeSlot(s);
            count_--;
        }

        void erase(uint64_t h2) {
            uint32_t* val = idx_.find(h2);
            if (!val) return;
            uint32_t s = *val;
            idx_.erase(h2);
            uint32_t p = slots_[s].prev, n = slots_[s].next;
            if (p != NIL) slots_[p].next = n;
            else head_ = n;
            if (n != NIL) slots_[n].prev = p;
            else tail_ = p;
            freeSlot(s);
            count_--;
        }

        void splice_front(uint64_t h2) {
            uint32_t* val = idx_.find(h2);
            if (!val) return;
            uint32_t s = *val;
            if (s == head_) return;
            uint32_t p = slots_[s].prev, n = slots_[s].next;
            if (p != NIL) slots_[p].next = n;
            if (n != NIL) slots_[n].prev = p;
            else tail_ = p;
            slots_[s].prev = NIL;
            slots_[s].next = head_;
            if (head_ != NIL) slots_[head_].prev = s;
            head_ = s;
        }

        HashPair back() const { return slots_[tail_].hashes; }
        size_t size() const { return count_; }
        bool empty() const { return count_ == 0; }
    };

    class ArcLists {
        static constexpr uint32_t NIL = UINT32_MAX;
        static constexpr uint8_t kT1 = 0, kT2 = 1, kB1 = 2, kB2 = 3;

        struct Slot {
            HashPair hashes;
            uint32_t prev;
            uint32_t next;
        };

        struct ListHead {
            uint32_t head = NIL;
            uint32_t tail = NIL;
            size_t count = 0;
        };

        std::vector<Slot> slots_;
        ListHead heads_[4];
        uint32_t freeHead_ = NIL;
        OpenIdx<uint32_t> idx_;

        static uint32_t packEntry(uint8_t listId, uint32_t slot) {
            return (static_cast<uint32_t>(listId) << 30) | slot;
        }
        static uint8_t unpackListId(uint32_t packed) { return packed >> 30; }
        static uint32_t unpackSlot(uint32_t packed) { return packed & 0x3FFFFFFF; }

        uint32_t allocSlot() {
            uint32_t s = freeHead_;
            freeHead_ = slots_[s].next;
            return s;
        }

        void freeSlot(uint32_t s) {
            slots_[s].hashes = {};
            slots_[s].prev = NIL;
            slots_[s].next = freeHead_;
            freeHead_ = s;
        }

        void doPushFront(uint8_t lid, HashPair hashes) {
            uint32_t s = allocSlot();
            slots_[s].hashes = hashes;
            slots_[s].prev = NIL;
            slots_[s].next = heads_[lid].head;
            if (heads_[lid].head != NIL) slots_[heads_[lid].head].prev = s;
            else heads_[lid].tail = s;
            heads_[lid].head = s;
            idx_.insert(hashes.h2, packEntry(lid, s));
            heads_[lid].count++;
        }

        void doPopBack(uint8_t lid) {
            if (heads_[lid].tail == NIL) return;
            uint32_t s = heads_[lid].tail;
            idx_.erase(slots_[s].hashes.h2);
            uint32_t p = slots_[s].prev;
            if (p != NIL) slots_[p].next = NIL;
            else heads_[lid].head = NIL;
            heads_[lid].tail = p;
            freeSlot(s);
            heads_[lid].count--;
        }

        void doErase(uint8_t lid, uint32_t s) {
            uint32_t p = slots_[s].prev, n = slots_[s].next;
            if (p != NIL) slots_[p].next = n;
            else heads_[lid].head = n;
            if (n != NIL) slots_[n].prev = p;
            else heads_[lid].tail = p;
            freeSlot(s);
            heads_[lid].count--;
        }

        void doSpliceFront(uint8_t lid, uint32_t s) {
            if (s == heads_[lid].head) return;
            uint32_t p = slots_[s].prev, n = slots_[s].next;
            if (p != NIL) slots_[p].next = n;
            if (n != NIL) slots_[n].prev = p;
            else heads_[lid].tail = p;
            slots_[s].prev = NIL;
            slots_[s].next = heads_[lid].head;
            if (heads_[lid].head != NIL) slots_[heads_[lid].head].prev = s;
            heads_[lid].head = s;
        }

        bool findEntry(uint64_t h2, uint8_t& outList, uint32_t& outSlot) const {
            const uint32_t* v = idx_.find(h2);
            if (!v) return false;
            outList = unpackListId(*v);
            outSlot = unpackSlot(*v);
            return true;
        }

    public:
        explicit ArcLists(size_t capPerList)
            : idx_(capPerList * 2)
        {
            size_t totalSlots = capPerList * 2;
            slots_.resize(totalSlots);
            for (size_t i = 0; i < totalSlots; i++) {
                slots_[i] = {{}, NIL, static_cast<uint32_t>(i + 1)};
            }
            slots_[totalSlots - 1].next = NIL;
            freeHead_ = 0;
        }

        bool containsT1orT2(uint64_t h2) const noexcept {
            uint8_t lid; uint32_t s;
            return findEntry(h2, lid, s) && (lid == kT1 || lid == kT2);
        }

        bool containsB1(uint64_t h2) const noexcept {
            uint8_t lid; uint32_t s;
            return findEntry(h2, lid, s) && lid == kB1;
        }

        bool containsB2(uint64_t h2) const noexcept {
            uint8_t lid; uint32_t s;
            return findEntry(h2, lid, s) && lid == kB2;
        }

        void erase(uint64_t h2) noexcept {
            const uint32_t* v = idx_.find(h2);
            if (!v) return;
            doErase(unpackListId(*v), unpackSlot(*v));
            idx_.erase(h2);
        }

        void pushT1(HashPair hashes) { doPushFront(kT1, hashes); }
        void pushT2(HashPair hashes) { doPushFront(kT2, hashes); }
        void pushB1(HashPair hashes) { doPushFront(kB1, hashes); }
        void pushB2(HashPair hashes) { doPushFront(kB2, hashes); }

        void popBackT1() { doPopBack(kT1); }
        void popBackT2() { doPopBack(kT2); }
        void popBackB1() { doPopBack(kB1); }
        void popBackB2() { doPopBack(kB2); }

        HashPair backT1() const { return slots_[heads_[kT1].tail].hashes; }
        HashPair backT2() const { return slots_[heads_[kT2].tail].hashes; }
        HashPair backB1() const { return slots_[heads_[kB1].tail].hashes; }
        HashPair backB2() const { return slots_[heads_[kB2].tail].hashes; }

        size_t sizeT1() const { return heads_[kT1].count; }
        size_t sizeT2() const { return heads_[kT2].count; }
        size_t sizeB1() const { return heads_[kB1].count; }
        size_t sizeB2() const { return heads_[kB2].count; }
        bool emptyT1() const { return heads_[kT1].count == 0; }
        bool emptyT2() const { return heads_[kT2].count == 0; }
        bool emptyB1() const { return heads_[kB1].count == 0; }
        bool emptyB2() const { return heads_[kB2].count == 0; }

        void promoteT1toT2(HashPair hashes) {
            const uint32_t* v = idx_.find(hashes.h2);
            if (!v || unpackListId(*v) != kT1) return;
            doErase(kT1, unpackSlot(*v));
            idx_.erase(hashes.h2);
            doPushFront(kT2, hashes);
        }

        void promoteB1toT2(HashPair hashes) {
            const uint32_t* v = idx_.find(hashes.h2);
            if (!v || unpackListId(*v) != kB1) return;
            doErase(kB1, unpackSlot(*v));
            idx_.erase(hashes.h2);
            doPushFront(kT2, hashes);
        }

        void promoteB2toT2(HashPair hashes) {
            const uint32_t* v = idx_.find(hashes.h2);
            if (!v || unpackListId(*v) != kB2) return;
            doErase(kB2, unpackSlot(*v));
            idx_.erase(hashes.h2);
            doPushFront(kT2, hashes);
        }

        void spliceFrontT2(uint64_t h2) {
            const uint32_t* v = idx_.find(h2);
            if (!v || unpackListId(*v) != kT2) return;
            doSpliceFront(kT2, unpackSlot(*v));
        }

        uint8_t whichList(uint64_t h2) const {
            const uint32_t* v = idx_.find(h2);
            return v ? unpackListId(*v) : 0xFF;
        }
    };

    class PromoteBuf {
        static constexpr size_t kCapacity = 4096;
        static constexpr size_t kThreshold = 1024;

        struct Slot {
            HashPair hashes;
            std::atomic<bool> ready{false};
        };

        std::vector<Slot> slots_;
        std::atomic<size_t> writeHead_{0};
        size_t drainTail_ = 0;
        std::function<void()> wakeCb_;

    public:
        PromoteBuf() : slots_(kCapacity) {}

        PromoteBuf(const PromoteBuf&) = delete;
        PromoteBuf& operator=(const PromoteBuf&) = delete;

        void setWakeCallback(std::function<void()> cb) { wakeCb_ = std::move(cb); }

        size_t pending() const {
            return writeHead_.load(std::memory_order_relaxed) - drainTail_;
        }

        bool enqueue(HashPair hashes) {
            size_t pos = writeHead_.fetch_add(1, std::memory_order_relaxed);
            size_t idx = pos % kCapacity;
            if (slots_[idx].ready.load(std::memory_order_acquire)) return false;
            slots_[idx].hashes = hashes;
            slots_[idx].ready.store(true, std::memory_order_release);
            if (pending() >= kThreshold && wakeCb_) wakeCb_();
            return true;
        }

        template <typename Fn>
        void drainWith(Fn&& fn) {
            size_t head = writeHead_.load(std::memory_order_acquire);
            while (drainTail_ < head) {
                size_t idx = drainTail_ % kCapacity;
                if (!slots_[idx].ready.load(std::memory_order_acquire)) break;
                slots_[idx].ready.store(false, std::memory_order_relaxed);
                fn(slots_[idx].hashes);
                ++drainTail_;
            }
        }
    };

    using CMapAllocFn = void*(*)(size_t);
    using CMapFreeFn = void(*)(void*, size_t);

    inline void* CMapDefaultAlloc(size_t size) {
        return ::operator new(size, std::align_val_t{64});
    }
    inline void CMapDefaultFree(void* ptr, size_t) {
        ::operator delete(ptr, std::align_val_t{64});
    }

    // CMapSetResult: Set() returns whether the operation was an INSERT (new key)
    // or UPDATE (existing key). This eliminates the need for a separate Find()
    // call before Set() — the caller can branch on INSERT vs UPDATE from a
    // single probe. Without this, ConcurrentARC::Set() would probe twice:
    // once in Find() to check existence, once in Set() to write. Single-probe
    // saves ~50% of hash table lookup overhead per Set() operation.
    struct CMapSetResult {
        Error err;
        bool inserted; // true = INSERT (new slot), false = UPDATE (existing slot)
    };

    enum class CMapCtrlState : uint8_t //<-- Exact width.
    {
        kEmpty = 0x80,
        kDeleted = 0xFE,
        kSentinel = 0xFF
    };

    struct ProbeResult {
        size_t matchSlot = SIZE_MAX;   // SIZE_MAX if not found
        size_t deletedSlot = SIZE_MAX; // SIZE_MAX if no deleted found
        size_t emptySlot = SIZE_MAX;   // SIZE_MAX if no empty found
    };

    // h2 is also used for fingerprint. To reduce colisions. We are reducing the probability, but we can't remove it.

    inline HashPair HashKey(std::string_view key) {
        XXH128_hash_t h = XXH3_128bits(key.data(), key.size());
        return {h.high64, h.low64};
    }

    // Swiss table varient, For NUMA backed bins create in the node.
    // Furrballs currently allocates the Entire Node metadata in the node's memory and we must ensure that it stays that
    // way or have a dynamic allocator.
    // TODO: re-add back the Key in the template, not needed for now.
    template <class Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class CMap {
      private:
        static constexpr unsigned int kGroupSize = 16;

        struct alignas(64) Slot {
            static constexpr bool kNeedsSplit = (16 + sizeof(Value)) > 64;

            std::atomic<uint8_t> seq;
            uint8_t padding[7];
            std::atomic<uint64_t> fingerprint;

            alignas(kNeedsSplit ? 64 : alignof(Value)) Value value;
        };

        const uint8_t* Ctrl() const { return ctrl_; }
        Slot* Slots() { return slots_; }
        const Slot* Slots() const { return slots_; }

        Slot* slots_ = nullptr;
        uint8_t* ctrl_ = nullptr;
        CMapAllocFn allocFn_ = nullptr;
        CMapFreeFn freeFn_ = nullptr;
        size_t numGroups_ = 0;
        size_t slotCount_ = 0;

      public:
        CMap(size_t capacity,
             CMapAllocFn af = CMapDefaultAlloc,
             CMapFreeFn ff = CMapDefaultFree)
            : allocFn_(af), freeFn_(ff)
        {
            size_t allocCap = 1;
            while (allocCap < capacity) allocCap <<= 1;

            numGroups_ = allocCap / kGroupSize;
            slotCount_ = allocCap;

            slots_ = static_cast<Slot*>(allocFn_(slotCount_ * sizeof(Slot)));
            for (size_t i = 0; i < slotCount_; ++i)
                new (&slots_[i]) Slot{};
            ctrl_ = static_cast<uint8_t*>(allocFn_(slotCount_ + kGroupSize));

            memset(ctrl_, 0x80, slotCount_ + kGroupSize);
            for (size_t i = slotCount_; i < slotCount_ + kGroupSize; i++) {
                ctrl_[i] = 0xFF;
            }
        }

        ~CMap() {
            if (slots_) freeFn_(slots_, slotCount_ * sizeof(Slot));
            if (ctrl_) freeFn_(ctrl_, slotCount_ + kGroupSize);
        }

        int MatchCtrlMask(__m128i ctrl_vec, uint8_t target) const noexcept {
            return _mm_movemask_epi8(_mm_cmpeq_epi8(ctrl_vec, _mm_set1_epi8(target)));
        }

        // x86-64 guarantees byte-aligned stores are atomic.
        // SIMD load + concurrent CAS on same ctrl byte is safe in practice.
        bool CasCtrl(size_t slotIdx, uint8_t expected, uint8_t desired) noexcept {
            return __atomic_compare_exchange_n(
                &ctrl_[slotIdx], &expected, desired,
                false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
        }

        template <bool FindDeleted = false> ProbeResult Probe(const HashPair& hashes) const noexcept {
            ProbeResult result;
            auto group = hashes.h1 & (numGroups_ - 1);
            auto h2_short = static_cast<uint8_t>(hashes.h2 >> 57);

            for (size_t probeCount = 0; probeCount < numGroups_; ++probeCount) {
                __m128i ctrl_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(Ctrl() + group * kGroupSize));
                int matchMask = MatchCtrlMask(ctrl_vec, h2_short);
                while (matchMask != 0) {
                    int i = std::countr_zero(static_cast<unsigned>(matchMask));
                    matchMask &= matchMask - 1;
                    size_t slotIdx = group * kGroupSize + static_cast<size_t>(i);
                    if (Slots()[slotIdx].fingerprint.load(std::memory_order_relaxed) == hashes.h2) {
                        result.matchSlot = slotIdx;
                        return result;
                    }
                }
                
                // Check for DELETED (only needed by Insert for tombstone reuse)
                if constexpr (FindDeleted) {
                    if (result.deletedSlot == SIZE_MAX) {
                        int delMask = MatchCtrlMask(ctrl_vec, static_cast<uint8_t>(CMapCtrlState::kDeleted));
                        if (delMask != 0) {
                            result.deletedSlot = group * kGroupSize + 
                                static_cast<size_t>(std::countr_zero(static_cast<unsigned>(delMask)));
                        }
                    }
                }

                int emptyMask = MatchCtrlMask(ctrl_vec, static_cast<uint8_t>(CMapCtrlState::kEmpty));
                if (emptyMask != 0) {
                    result.emptySlot = group * kGroupSize + 
                        static_cast<size_t>(std::countr_zero(static_cast<unsigned>(emptyMask)));
                    return result;
                }

                group = (group + 1) & (numGroups_ - 1);
            }
            return result; 
        }

        std::optional<Value> Find(std::string_view key) const noexcept {
            HashPair pair = HashKey(key);
            return FindByHash(pair);
        }

        std::optional<Value> FindByHash(const HashPair& pair) const noexcept {
            ProbeResult result = Probe<false>(pair);
            if (result.matchSlot == SIZE_MAX) return std::nullopt;
            const Slot& slot = Slots()[result.matchSlot];
            alignas(Value) std::byte buf[sizeof(Value)];
            uint8_t s1;
            do {
                s1 = slot.seq.load(std::memory_order_acquire);
                if (s1 & 1) { _mm_pause(); continue; }
                std::memcpy(buf, &slot.value, sizeof(Value));
            } while (s1 != slot.seq.load(std::memory_order_acquire));
            return std::bit_cast<Value>(buf);
        }
        CMapSetResult Set(std::string_view key, const Value& val) noexcept {
            HashPair pair = HashKey(key);
            auto h2_short = static_cast<uint8_t>(pair.h2 >> 57);
            ProbeResult result = Probe<true>(pair);
            if(result.deletedSlot == SIZE_MAX && result.matchSlot == SIZE_MAX && result.emptySlot == SIZE_MAX){
                return {CACHE_FULL, false};
            }

            if(result.matchSlot != SIZE_MAX){
                Slot& targetSlot = Slots()[result.matchSlot];
                uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
                if (expected & 1) return {ABANDONED_SET, false};
                if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return {ABANDONED_SET, false};

                targetSlot.fingerprint.store(pair.h2, std::memory_order_relaxed);
                std::memcpy(&targetSlot.value, &val, sizeof(Value));
                targetSlot.seq.store(expected + 2, std::memory_order_release);
                return {NO_ERR, false};
            }

            size_t targetIdx = result.deletedSlot != SIZE_MAX ? result.deletedSlot : result.emptySlot;
            Slot& targetSlot = Slots()[targetIdx];

            uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
            if (expected & 1) return {ABANDONED_SET, false};
            if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                return {ABANDONED_SET, false};

            targetSlot.fingerprint.store(pair.h2, std::memory_order_relaxed);
            if (!CasCtrl(targetIdx, static_cast<uint8_t>(CMapCtrlState::kDeleted), h2_short) &&
                !CasCtrl(targetIdx, static_cast<uint8_t>(CMapCtrlState::kEmpty), h2_short)) {
                targetSlot.seq.store(expected, std::memory_order_release);
                return {ABANDONED_SET, false};
            }

            std::memcpy(&targetSlot.value, &val, sizeof(Value));
            targetSlot.seq.store(expected + 2, std::memory_order_release);
            return {NO_ERR, true};
        }

        // UpdateInPlace: acquires the slot's seqlock and calls fn(value).
        // The callback can modify the Value AND perform side effects (e.g. memcpy
        // to DataOffset) — all under the seqlock. This fixes the torn-read bug
        // where the old code wrote page data outside any seqlock window, allowing
        // concurrent readers to observe partially-written data with consistent metadata.
        template <typename Fn>
        Error UpdateInPlace(std::string_view key, Fn&& fn) noexcept {
            HashPair pair = HashKey(key);
            ProbeResult result = Probe<false>(pair);
            if (result.matchSlot == SIZE_MAX) return KEY_NOT_FOUND;

            Slot& targetSlot = Slots()[result.matchSlot];
            uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
            if (expected & 1) return ABANDONED_SET;
            if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                return ABANDONED_SET;

            if (targetSlot.value.DataOffset == nullptr) {
                targetSlot.seq.store(expected, std::memory_order_release);
                return KEY_NOT_FOUND;
            }

            fn(targetSlot.value);

            targetSlot.seq.store(expected + 2, std::memory_order_release);
            return NO_ERR;
        }

        Error Erase(std::string_view key) noexcept {
            return FindAndErase(key).err;
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) noexcept {
            ProbeResult result = Probe<false>(hashes);
            if (result.matchSlot == SIZE_MAX) return KEY_NOT_FOUND;
            Slot& targetSlot = Slots()[result.matchSlot];
            uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
            if (expected & 1) return ABANDONED_SET;
            if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                return ABANDONED_SET;
            if (targetSlot.value.DataOffset == nullptr) {
                targetSlot.seq.store(expected, std::memory_order_release);
                return KEY_NOT_FOUND;
            }
            fn(targetSlot.value);
            targetSlot.seq.store(expected + 2, std::memory_order_release);
            return NO_ERR;
        }

        struct FindAndEraseResult {
            Error err = KEY_NOT_FOUND;
            std::optional<Value> value = std::nullopt;
        };

        FindAndEraseResult FindAndErase(std::string_view key) noexcept {
            FindAndEraseResult out;
            HashPair pair = HashKey(key);
            ProbeResult result = Probe<false>(pair);
            if (result.matchSlot == SIZE_MAX) return out;

            Slot& targetSlot = Slots()[result.matchSlot];
            uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
            if (expected & 1) { out.err = ABANDONED_SET; return out; }
            if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                out.err = ABANDONED_SET;
                return out;
            }

            if (!CasCtrl(result.matchSlot, static_cast<uint8_t>(pair.h2 >> 57),
                    static_cast<uint8_t>(CMapCtrlState::kDeleted))) {
                targetSlot.seq.store(expected, std::memory_order_release);
                out.err = ABANDONED_SET;
                return out;
            }

            alignas(Value) std::byte buf[sizeof(Value)];
            std::memcpy(buf, &targetSlot.value, sizeof(Value));
            targetSlot.fingerprint.store(0, std::memory_order_relaxed);
            std::memset(&targetSlot.value, 0, sizeof(Value));
            targetSlot.seq.store(expected + 2, std::memory_order_release);
            out.err = NO_ERR;
            out.value = std::bit_cast<Value>(buf);
            return out;
        }

        FindAndEraseResult FindAndEraseByHash(const HashPair& hashes) noexcept {
            FindAndEraseResult out;
            ProbeResult result = Probe<false>(hashes);
            if (result.matchSlot == SIZE_MAX) return out;

            Slot& targetSlot = Slots()[result.matchSlot];
            uint8_t expected = targetSlot.seq.load(std::memory_order_acquire);
            if (expected & 1) { out.err = ABANDONED_SET; return out; }
            if (!targetSlot.seq.compare_exchange_strong(expected, expected + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                out.err = ABANDONED_SET;
                return out;
            }

            if (!CasCtrl(result.matchSlot, static_cast<uint8_t>(hashes.h2 >> 57),
                    static_cast<uint8_t>(CMapCtrlState::kDeleted))) {
                targetSlot.seq.store(expected, std::memory_order_release);
                out.err = ABANDONED_SET;
                return out;
            }

            alignas(Value) std::byte buf[sizeof(Value)];
            std::memcpy(buf, &targetSlot.value, sizeof(Value));
            targetSlot.fingerprint.store(0, std::memory_order_relaxed);
            std::memset(&targetSlot.value, 0, sizeof(Value));
            targetSlot.seq.store(expected + 2, std::memory_order_release);
            out.err = NO_ERR;
            out.value = std::bit_cast<Value>(buf);
            return out;
        }

    };

    template <class Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class ConcurrentARC {
    public:
        using EvictionCallback = std::function<void(const Value&)>;

    private:
        CMap<Value> store_;
        ArcLists lists_;
        SpinLock arcLock_;
        PromoteBuf promoteBuf_;
        size_t capacity_;
        size_t p_;
        EvictionCallback evictionCallback_ = [](const Value&) {};

        void drainPromoteBuf() {
            promoteBuf_.drainWith([&](HashPair hashes) {
                uint8_t lid = lists_.whichList(hashes.h2);
                if (lid == 0) {
                    lists_.promoteT1toT2(hashes);
                } else if (lid == 1) {
                    lists_.spliceFrontT2(hashes.h2);
                }
            });
        }

        bool replaceLocked(uint64_t h2) {
            if (!lists_.emptyT1() && (lists_.sizeT1() > p_ ||
                (lists_.containsB2(h2) && lists_.sizeT1() == p_))) {
                HashPair old = lists_.backT1();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                lists_.popBackT1();
                lists_.pushB1(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            if (!lists_.emptyT2()) {
                HashPair old = lists_.backT2();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                lists_.popBackT2();
                lists_.pushB2(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            return true;
        }

        bool evictLocked() {
            if (lists_.sizeT1() + lists_.sizeB1() >= capacity_) {
                if (lists_.sizeT1() < capacity_ && !lists_.emptyB1()) {
                    lists_.popBackB1();
                } else if (!lists_.emptyT1()) {
                    HashPair hashes = lists_.backT1();
                    auto result = store_.FindAndEraseByHash(hashes);
                    if (result.err != NO_ERR) return false;
                    lists_.popBackT1();
                    if (result.value) evictionCallback_(*result.value);
                }
            }
            if (lists_.sizeT1() + lists_.sizeT2() + lists_.sizeB1() + lists_.sizeB2() >= 2 * capacity_) {
                if (lists_.sizeT2() + lists_.sizeB2() > capacity_ && !lists_.emptyB2()) {
                    lists_.popBackB2();
                } else if (!lists_.emptyT2()) {
                    HashPair hashes = lists_.backT2();
                    auto result = store_.FindAndEraseByHash(hashes);
                    if (result.err != NO_ERR) return false;
                    lists_.popBackT2();
                    if (result.value) evictionCallback_(*result.value);
                }
            }
            return true;
        }

    public:
        ConcurrentARC(size_t cap, CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
            : store_(cap, af, ff), lists_(cap),
              capacity_(cap), p_(0) {}

        void setWakeCallback(std::function<void()> cb) { promoteBuf_.setWakeCallback(std::move(cb)); }

        void drainPromotes() {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();
        }

        void SetEvictionCallback(EvictionCallback cb) { evictionCallback_ = cb; }

        bool ForceEvictOne() {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();
            if (!lists_.emptyT1() && (lists_.sizeT1() > p_ || lists_.emptyT2())) {
                HashPair old = lists_.backT1();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                lists_.popBackT1();
                lists_.pushB1(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            if (!lists_.emptyT2()) {
                HashPair old = lists_.backT2();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                lists_.popBackT2();
                lists_.pushB2(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            return false;
        }

        uint8_t GetDesire(uint64_t) const { return 0; }

        std::optional<Value> Find(const std::string& key) {
            HashPair hashes = HashKey(key);
            auto val = store_.FindByHash(hashes);
            if (!val) return std::nullopt;
            promoteBuf_.enqueue(hashes);
            return val;
        }

        template <typename Fn>
        Error UpdateInPlace(const std::string& key, Fn&& fn) {
            return store_.UpdateInPlace(key, std::forward<Fn>(fn));
        }

        template <typename Fn>
        Error FindAndUpdateInPlace(const std::string& key, size_t maxDataSize, Fn&& fn) {
            std::lock_guard<SpinLock> guard(arcLock_);
            auto existing = store_.Find(key);
            if (!existing.has_value() || existing->DataOffset == nullptr) return KEY_NOT_FOUND;
            if (maxDataSize > existing->DataSize) return KEY_NOT_FOUND;
            fn(*existing);
            return NO_ERR;
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) {
            return store_.UpdateInPlaceByHash(hashes, std::forward<Fn>(fn));
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();
            HashPair hashes = HashKey(key);
            lists_.erase(hashes.h2);
            return store_.FindAndErase(key);
        }

        typename CMap<Value>::FindAndEraseResult EraseByHash(const HashPair& hashes) {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();
            lists_.erase(hashes.h2);
            return store_.FindAndEraseByHash(hashes);
        }

        bool MigrateAndLeaveSentinel(const HashPair& hashes, int destNode) {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();
            lists_.erase(hashes.h2);
            return store_.FindAndEraseByHash(hashes);
        }

        Error Set(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();

            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            CMapSetResult result = store_.Set(key, val);
            if (result.err != NO_ERR) return result.err;

            if (!result.inserted) {
                uint8_t lid = lists_.whichList(h2);
                if (lid == 0) {
                    lists_.promoteT1toT2(hashes);
                } else if (lid == 1) {
                    lists_.spliceFrontT2(h2);
                }
                return NO_ERR;
            }

            if (lists_.containsB1(h2)) {
                size_t delta1 = lists_.sizeB1() > 0 ? lists_.sizeB2() / lists_.sizeB1() : 1;
                p_ = std::min(capacity_, p_ + std::max(delta1, (size_t)1));
                if (!replaceLocked(h2)) return ABANDONED_SET;
                lists_.promoteB1toT2(hashes);
                return NO_ERR;
            }

            if (lists_.containsB2(h2)) {
                size_t delta2 = lists_.sizeB2() > 0 ? lists_.sizeB1() / lists_.sizeB2() : 1;
                size_t dec = std::max(delta2, (size_t)1);
                p_ = (p_ >= dec) ? p_ - dec : 0;
                if (!replaceLocked(h2)) return ABANDONED_SET;
                lists_.promoteB2toT2(hashes);
                return NO_ERR;
            }

            if (!evictLocked()) return ABANDONED_SET;
            lists_.pushT1(hashes);
            return NO_ERR;
        }

        Error EvictAndSet(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(arcLock_);
            drainPromoteBuf();

            bool evicted = false;
            if (!lists_.emptyT1() && (lists_.sizeT1() > p_ || lists_.emptyT2())) {
                HashPair old = lists_.backT1();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err == NO_ERR) {
                    lists_.popBackT1();
                    lists_.pushB1(old);
                    if (result.value) evictionCallback_(*result.value);
                    evicted = true;
                }
            }
            if (!evicted && !lists_.emptyT2()) {
                HashPair old = lists_.backT2();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err == NO_ERR) {
                    lists_.popBackT2();
                    lists_.pushB2(old);
                    if (result.value) evictionCallback_(*result.value);
                    evicted = true;
                }
            }
            if (!evicted) return ABANDONED_SET;

            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            CMapSetResult result = store_.Set(key, val);
            if (result.err != NO_ERR) return result.err;

            if (!result.inserted) {
                uint8_t lid = lists_.whichList(h2);
                if (lid == 0) {
                    lists_.promoteT1toT2(hashes);
                } else if (lid == 1) {
                    lists_.spliceFrontT2(h2);
                }
                return NO_ERR;
            }

            if (lists_.containsB1(h2)) {
                size_t delta1 = lists_.sizeB1() > 0 ? lists_.sizeB2() / lists_.sizeB1() : 1;
                p_ = std::min(capacity_, p_ + std::max(delta1, (size_t)1));
                if (!replaceLocked(h2)) return ABANDONED_SET;
                lists_.promoteB1toT2(hashes);
                return NO_ERR;
            }

            if (lists_.containsB2(h2)) {
                size_t delta2 = lists_.sizeB2() > 0 ? lists_.sizeB1() / lists_.sizeB2() : 1;
                size_t dec = std::max(delta2, (size_t)1);
                p_ = (p_ >= dec) ? p_ - dec : 0;
                if (!replaceLocked(h2)) return ABANDONED_SET;
                lists_.promoteB2toT2(hashes);
                return NO_ERR;
            }

            if (!evictLocked()) return ABANDONED_SET;
            lists_.pushT1(hashes);
            return NO_ERR;
        }
    };

    template <class Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class FragmentedCMapStore {
        struct alignas(64) Segment {
            ConcurrentARC<Value> arc;
            Segment(size_t cap, CMapAllocFn af, CMapFreeFn ff) : arc(cap, af, ff) {}
        };

        std::vector<std::unique_ptr<Segment>> segments_;
        size_t numSegments_;

    public:
        using EvictionCallback = std::function<void(const Value&)>;

        FragmentedCMapStore(size_t totalCapacity, size_t numSegments,
                            CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
            : numSegments_(numSegments) {
            size_t perSeg = totalCapacity / numSegments;
            for (size_t i = 0; i < numSegments; i++) {
                segments_.push_back(std::make_unique<Segment>(perSeg, af, ff));
            }
        }

        void SetEvictionCallback(EvictionCallback cb) {
            for (auto& seg : segments_) seg->arc.SetEvictionCallback(cb);
        }
        void setWakeCallback(std::function<void()>) {}
        void drainPromotes() {}
        uint8_t GetDesire(uint64_t) const { return 0; }

        std::optional<Value> Find(const std::string& key) {
            HashPair hashes = HashKey(key);
            return segments_[hashes.h2 % numSegments_]->arc.Find(key);
        }

        Error Set(const std::string& key, const Value& val) {
            HashPair hashes = HashKey(key);
            return segments_[hashes.h2 % numSegments_]->arc.Set(key, val);
        }

        bool ForceEvictOne() {
            for (auto& seg : segments_) {
                if (seg->arc.ForceEvictOne()) return true;
            }
            return false;
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            HashPair hashes = HashKey(key);
            return segments_[hashes.h2 % numSegments_]->arc.Erase(key);
        }

        auto EraseByHash(const HashPair& hashes) {
            return segments_[hashes.h2 % numSegments_]->arc.EraseByHash(hashes);
        }

        template <typename Fn>
        Error UpdateInPlace(const std::string& key, Fn&& fn) {
            HashPair hashes = HashKey(key);
            return segments_[hashes.h2 % numSegments_]->arc.UpdateInPlace(key, std::forward<Fn>(fn));
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) {
            return segments_[hashes.h2 % numSegments_]->arc.UpdateInPlaceByHash(hashes, std::forward<Fn>(fn));
        }

        bool MigrateAndLeaveSentinel(const HashPair&, int) { return false; }
        int FindSentinel(const HashPair&) const { return -1; }
    };

} // namespace NuAtlas
