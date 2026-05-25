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
#include <queue>
#include <cmath>

namespace NuAtlas {

    class FlatList {
        static constexpr uint32_t NIL = UINT32_MAX;

        struct Slot {
            HashPair hashes;
            uint32_t prev;
            uint32_t next;
        };

        std::vector<Slot> slots_;
        std::unordered_map<uint64_t, uint32_t> idx_;
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
        explicit FlatList(size_t maxEntries) {
            slots_.resize(maxEntries);
            for (size_t i = 0; i < maxEntries; i++) {
                slots_[i] = {{}, NIL, static_cast<uint32_t>(i + 1)};
            }
            slots_[maxEntries - 1].next = NIL;
            freeHead_ = 0;
            idx_.reserve(maxEntries);
        }

        bool contains(uint64_t h2) const {
            return idx_.find(h2) != idx_.end();
        }

        void push_front(HashPair hashes) {
            uint32_t s = allocSlot();
            slots_[s].hashes = hashes;
            slots_[s].prev = NIL;
            slots_[s].next = head_;
            if (head_ != NIL) slots_[head_].prev = s;
            else tail_ = s;
            head_ = s;
            idx_[hashes.h2] = s;
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
            auto it = idx_.find(h2);
            if (it == idx_.end()) return;
            uint32_t s = it->second;
            idx_.erase(it);
            uint32_t p = slots_[s].prev, n = slots_[s].next;
            if (p != NIL) slots_[p].next = n;
            else head_ = n;
            if (n != NIL) slots_[n].prev = p;
            else tail_ = p;
            freeSlot(s);
            count_--;
        }

        void splice_front(uint64_t h2) {
            auto it = idx_.find(h2);
            if (it == idx_.end()) return;
            uint32_t s = it->second;
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

    // TODO: Per-thread sharded PromoteBuf for high thread counts (100+).
    // Current single writeHead_ scales to ~20 threads per node. For production:
    //   struct PromoteRegistry {
    //       std::atomic<PromoteBuf*> head{nullptr};
    //       PromoteBuf& get() {
    //           thread_local PromoteBuf* mine = nullptr;
    //           if (!mine) {
    //               mine = new PromoteBuf();
    //               mine->next = head.load(relaxed);
    //               while (!head.compare_exchange_weak(mine->next, mine, release, relaxed));
    //           }
    //           return *mine;
    //       }
    //       void drainAll(FlatList& t1, FlatList& t2) {
    //           for (auto* buf = head.load(acquire); buf; buf = buf->next)
    //               buf->drain(t1, t2);
    //       }
    //   };
    // Each thread gets its own buffer on first Get(). Zero shared contention on
    // enqueue. Drain iterates the linked list under SpinLock. No max thread count
    // needed. Acceptable memory leak on thread exit for server workloads.

    class PromoteBuf {
        static constexpr size_t kCapacity = 256;
        static constexpr size_t kDrainEvery = 64;

        struct Slot {
            HashPair hashes;
            std::atomic<bool> ready{false};
        };

        std::array<Slot, kCapacity> slots_;
        std::atomic<size_t> writeHead_{0};
        size_t drainTail_ = 0;

    public:
        bool enqueue(HashPair hashes) {
            size_t pos = writeHead_.fetch_add(1, std::memory_order_relaxed);
            size_t idx = pos % kCapacity;
            if (slots_[idx].ready.load(std::memory_order_acquire)) return false;
            slots_[idx].hashes = hashes;
            slots_[idx].ready.store(true, std::memory_order_release);
            return (pos + 1) % kDrainEvery != 0;
        }

        void drain(FlatList& t1, FlatList& t2) {
            size_t head = writeHead_.load(std::memory_order_acquire);
            size_t drained = 0;
            while (drainTail_ < head && drained < kCapacity) {
                size_t idx = drainTail_ % kCapacity;
                if (!slots_[idx].ready.load(std::memory_order_acquire)) break;
                slots_[idx].ready.store(false, std::memory_order_relaxed);
                uint64_t h2 = slots_[idx].hashes.h2;
                if (t1.contains(h2)) {
                    t1.erase(h2);
                    t2.push_front(slots_[idx].hashes);
                } else if (t2.contains(h2)) {
                    t2.splice_front(h2);
                }
                ++drainTail_;
                ++drained;
            }
        }

        void drainSingle(FlatList& lru) {
            size_t head = writeHead_.load(std::memory_order_acquire);
            size_t drained = 0;
            while (drainTail_ < head && drained < kCapacity) {
                size_t idx = drainTail_ % kCapacity;
                if (!slots_[idx].ready.load(std::memory_order_acquire)) break;
                slots_[idx].ready.store(false, std::memory_order_relaxed);
                uint64_t h2 = slots_[idx].hashes.h2;
                if (lru.contains(h2)) {
                    lru.splice_front(h2);
                }
                ++drainTail_;
                ++drained;
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
        FlatList t1_, t2_, b1_, b2_;
        SpinLock arcLock_;
        PromoteBuf promoteBuf_;
        size_t capacity_;
        size_t p_;
        EvictionCallback evictionCallback_ = [](const Value&) {};

        bool replaceLocked(uint64_t h2) {
            if (!t1_.empty() && (t1_.size() > p_ ||
                (b2_.contains(h2) && t1_.size() == p_))) {
                HashPair old = t1_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                t1_.pop_back();
                b1_.push_front(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            if (!t2_.empty()) {
                HashPair old = t2_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                t2_.pop_back();
                b2_.push_front(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            return true;
        }

        bool evictLocked() {
            if (t1_.size() + b1_.size() >= capacity_) {
                if (t1_.size() < capacity_ && !b1_.empty()) {
                    b1_.pop_back();
                } else if (!t1_.empty()) {
                    HashPair hashes = t1_.back();
                    auto result = store_.FindAndEraseByHash(hashes);
                    if (result.err != NO_ERR) return false;
                    t1_.pop_back();
                    if (result.value) evictionCallback_(*result.value);
                }
            }
            if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * capacity_) {
                if (t2_.size() + b2_.size() > capacity_ && !b2_.empty()) {
                    b2_.pop_back();
                } else if (!t2_.empty()) {
                    HashPair hashes = t2_.back();
                    auto result = store_.FindAndEraseByHash(hashes);
                    if (result.err != NO_ERR) return false;
                    t2_.pop_back();
                    if (result.value) evictionCallback_(*result.value);
                }
            }
            return true;
        }

    public:
        ConcurrentARC(size_t cap, CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
            : store_(cap, af, ff), t1_(2 * cap), t2_(2 * cap), b1_(2 * cap), b2_(2 * cap),
              capacity_(cap), p_(0) {}

        void SetEvictionCallback(EvictionCallback cb) { evictionCallback_ = cb; }

        bool ForceEvictOne() {
            std::lock_guard<SpinLock> guard(arcLock_);
            promoteBuf_.drain(t1_, t2_);
            if (!t1_.empty() && (t1_.size() > p_ || t2_.empty())) {
                HashPair old = t1_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                t1_.pop_back();
                b1_.push_front(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            if (!t2_.empty()) {
                HashPair old = t2_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err != NO_ERR) return false;
                t2_.pop_back();
                b2_.push_front(old);
                if (result.value) evictionCallback_(*result.value);
                return true;
            }
            return false;
        }

        uint8_t GetDesire(uint64_t) const { return 0; }

        std::optional<Value> Find(const std::string& key) {
            auto val = store_.Find(key);
            if (!val) return std::nullopt;

            HashPair hashes = HashKey(key);
            if (!promoteBuf_.enqueue(hashes)) {
                if (arcLock_.try_lock()) {
                    promoteBuf_.drain(t1_, t2_);
                    arcLock_.unlock();
                }
            }
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
            promoteBuf_.drain(t1_, t2_);
            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            t1_.erase(h2);
            t2_.erase(h2);
            b1_.erase(h2);
            b2_.erase(h2);
            return store_.FindAndErase(key);
        }

        typename CMap<Value>::FindAndEraseResult EraseByHash(const HashPair& hashes) {
            std::lock_guard<SpinLock> guard(arcLock_);
            promoteBuf_.drain(t1_, t2_);
            uint64_t h2 = hashes.h2;
            t1_.erase(h2);
            t2_.erase(h2);
            b1_.erase(h2);
            b2_.erase(h2);
            return store_.FindAndEraseByHash(hashes);
        }

        Error Set(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(arcLock_);
            promoteBuf_.drain(t1_, t2_);

            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            CMapSetResult result = store_.Set(key, val);
            if (result.err != NO_ERR) return result.err;

            if (!result.inserted) {
                if (t1_.contains(h2)) {
                    t1_.erase(h2);
                    t2_.push_front(hashes);
                } else if (t2_.contains(h2)) {
                    t2_.splice_front(h2);
                }
                return NO_ERR;
            }

            if (b1_.contains(h2)) {
                size_t delta1 = b1_.size() > 0 ? b2_.size() / b1_.size() : 1;
                p_ = std::min(capacity_, p_ + std::max(delta1, (size_t)1));
                if (!replaceLocked(h2)) return ABANDONED_SET;
                b1_.erase(h2);
                t2_.push_front(hashes);
                return NO_ERR;
            }

            if (b2_.contains(h2)) {
                size_t delta2 = b2_.size() > 0 ? b1_.size() / b2_.size() : 1;
                size_t dec = std::max(delta2, (size_t)1);
                p_ = (p_ >= dec) ? p_ - dec : 0;
                if (!replaceLocked(h2)) return ABANDONED_SET;
                b2_.erase(h2);
                t2_.push_front(hashes);
                return NO_ERR;
            }

            if (!evictLocked()) return ABANDONED_SET;
            t1_.push_front(hashes);
            return NO_ERR;
        }

        Error EvictAndSet(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(arcLock_);
            promoteBuf_.drain(t1_, t2_);

            bool evicted = false;
            if (!t1_.empty() && (t1_.size() > p_ || t2_.empty())) {
                HashPair old = t1_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err == NO_ERR) {
                    t1_.pop_back();
                    b1_.push_front(old);
                    if (result.value) evictionCallback_(*result.value);
                    evicted = true;
                }
            }
            if (!evicted && !t2_.empty()) {
                HashPair old = t2_.back();
                auto result = store_.FindAndEraseByHash(old);
                if (result.err == NO_ERR) {
                    t2_.pop_back();
                    b2_.push_front(old);
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
                if (t1_.contains(h2)) {
                    t1_.erase(h2);
                    t2_.push_front(hashes);
                } else if (t2_.contains(h2)) {
                    t2_.splice_front(h2);
                }
                return NO_ERR;
            }

            if (b1_.contains(h2)) {
                size_t delta1 = b1_.size() > 0 ? b2_.size() / b1_.size() : 1;
                p_ = std::min(capacity_, p_ + std::max(delta1, (size_t)1));
                if (!replaceLocked(h2)) return ABANDONED_SET;
                b1_.erase(h2);
                t2_.push_front(hashes);
                return NO_ERR;
            }

            if (b2_.contains(h2)) {
                size_t delta2 = b2_.size() > 0 ? b1_.size() / b2_.size() : 1;
                size_t dec = std::max(delta2, (size_t)1);
                p_ = (p_ >= dec) ? p_ - dec : 0;
                if (!replaceLocked(h2)) return ABANDONED_SET;
                b2_.erase(h2);
                t2_.push_front(hashes);
                return NO_ERR;
            }

            if (!evictLocked()) return ABANDONED_SET;
            t1_.push_front(hashes);
            return NO_ERR;
        }
    };

    static constexpr uint8_t AUG_REMARC_MAX = 15;
    static constexpr uint8_t AUG_DECAY_NUM = 7;
    static constexpr uint8_t AUG_DECAY_DEN = 8;

    static inline uint8_t augDecayR(uint8_t r) {
        return (uint8_t)((uint16_t)r * AUG_DECAY_NUM / AUG_DECAY_DEN);
    }
    static inline uint8_t augBoostR(uint8_t r) {
        uint16_t boosted = (uint16_t)r + (uint16_t)(AUG_REMARC_MAX - r) * 2 / 15;
        return boosted < AUG_REMARC_MAX ? (uint8_t)boosted : AUG_REMARC_MAX;
    }
    static inline uint8_t augBoostF(uint8_t f) {
        uint16_t boosted = (uint16_t)f + (uint16_t)(AUG_REMARC_MAX - f) * 1 / 15;
        return boosted < AUG_REMARC_MAX ? (uint8_t)boosted : AUG_REMARC_MAX;
    }
    static inline uint8_t augGetR(uint8_t s) { return s & 0xF; }
    static inline uint8_t augGetF(uint8_t s) { return (s >> 4) & 0xF; }
    static inline uint8_t augPack(uint8_t r, uint8_t f) { return (f << 4) | r; }

    // =================================================================
    //  AugAdaptScorer: value-agnostic prediction + adaptive switching
    // =================================================================
    //
    //  Tracks per-key prediction state (epoch, avgGap, r/f state) and
    //  provides eviction victim selection via adaptive heap/LRU switching.
    //  Operates purely on uint64_t h2 + HashPair — no Value dependency.
    //
    //  The cache calls:
    //    touchKey()      — on every access (hit or miss)
    //    selectVictim()  — during eviction, returns HashPair of best victim
    //    setTier()       — when moving keys between ARC lists
    //    onGhostHit()    — when a key is found in a ghost list
    //    eraseKey()      — permanent removal

    class AugAdaptScorer {
        static constexpr uint64_t TOMBSTONE = UINT64_MAX;
        static constexpr double STATS_ALPHA = 0.002;
        static constexpr double REGRET_ALPHA = 0.001;

        struct Slot {
            uint64_t h2 = 0;
            HashPair hashes;
            uint32_t lastEpoch = 0;
            uint32_t avgGap = 0;
            uint8_t state = 0;
            uint8_t tier = 0;
            uint64_t version = 0;
        };

        struct HeapEntry {
            uint64_t predictedNext;
            uint64_t h2;
            uint64_t version;
            bool operator<(const HeapEntry& o) const {
                if (predictedNext != o.predictedNext) return predictedNext < o.predictedNext;
                return version < o.version;
            }
        };

        std::vector<Slot> slots_;
        size_t mask_;
        std::priority_queue<HeapEntry> heap_;
        uint32_t epoch_ = 0;
        double popMeanGap_ = 0.0;
        double popM2Gap_ = 0.0;
        double regretEMA_ = 0.5;
        bool adaptive_ = false;

        Slot* probe(uint64_t h2) {
            size_t idx = h2 & mask_;
            while (true) {
                Slot& s = slots_[idx];
                if (s.h2 == h2) return &s;
                if (s.h2 == 0) return nullptr;
                idx = (idx + 1) & mask_;
            }
        }

        Slot* probeOrInsert(uint64_t h2) {
            size_t idx = h2 & mask_;
            size_t tomb = SIZE_MAX;
            while (true) {
                Slot& s = slots_[idx];
                if (s.h2 == h2) return &s;
                if (s.h2 == TOMBSTONE && tomb == SIZE_MAX) tomb = idx;
                if (s.h2 == 0) {
                    if (tomb != SIZE_MAX) idx = tomb;
                    slots_[idx] = {};
                    slots_[idx].h2 = h2;
                    return &slots_[idx];
                }
                idx = (idx + 1) & mask_;
            }
        }

        void pushToHeap(uint64_t h2) {
            if (!adaptive_) return;
            Slot* s = probe(h2);
            if (!s) return;
            s->version++;
            uint64_t pred = (s->avgGap > 0)
                ? (uint64_t)s->lastEpoch + s->avgGap
                : (uint64_t)epoch_;
            heap_.push({pred, h2, s->version});
        }

        void updateAdaptiveFlag() {
            double cv = (popMeanGap_ > 1.0)
                ? std::sqrt(std::max(0.0, popM2Gap_)) / popMeanGap_
                : 1.0;
            adaptive_ = (std::max(0.0, 1.0 - cv * 3.0) *
                         std::max(0.0, 1.0 - regretEMA_)) > 0.5;
        }

    public:
        AugAdaptScorer(size_t capacity) {
            size_t sz = 1;
            while (sz < capacity * 2) sz <<= 1;
            slots_.resize(sz);
            mask_ = sz - 1;
        }

        void touchKey(uint64_t h2, HashPair hp, uint8_t tier) {
            Slot* s = probeOrInsert(h2);
            s->hashes = hp;
            uint32_t gap = epoch_ - s->lastEpoch;
            s->lastEpoch = epoch_;
            if (gap > 0) s->avgGap = s->avgGap * 3 / 4 + gap / 4;

            uint8_t r = augGetR(s->state), f = augGetF(s->state);
            for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
            r = augBoostR(r);
            f = augBoostF(f);
            s->state = augPack(r, f);

            if (tier <= 2) {
                double g = (double)s->avgGap;
                double delta = g - popMeanGap_;
                popMeanGap_ += STATS_ALPHA * delta;
                popM2Gap_ += STATS_ALPHA * delta * (g - popMeanGap_);
                updateAdaptiveFlag();
            }
            pushToHeap(h2);
        }

        HashPair selectVictim(uint8_t targetTier) {
            if (adaptive_) {
                HashPair bestHashes{};
                uint64_t bestPred = 0;
                size_t attempts = 0;
                while (!heap_.empty() && attempts < 64) {
                    attempts++;
                    HeapEntry top = heap_.top();
                    Slot* s = probe(top.h2);
                    if (!s || s->version != top.version || s->tier != targetTier) {
                        heap_.pop(); continue;
                    }
                    if (top.predictedNext > bestPred) {
                        bestPred = top.predictedNext;
                        bestHashes = s->hashes;
                    }
                    heap_.pop();
                }
                if (bestHashes.h2 != 0) return bestHashes;
            }
            return {};
        }

        bool isAdaptive() const { return adaptive_; }

        void onGhostHit(uint64_t h2) {
            Slot* s = probe(h2);
            if (!s || s->avgGap == 0 || s->lastEpoch == 0) return;
            uint32_t actualGap = epoch_ - s->lastEpoch;
            double regret = (double)((actualGap > s->avgGap)
                ? (actualGap - s->avgGap) : (s->avgGap - actualGap))
                / (double)s->avgGap;
            regret = std::min(regret, 5.0);
            regretEMA_ = (1.0 - REGRET_ALPHA) * regretEMA_ + REGRET_ALPHA * regret;
            updateAdaptiveFlag();
        }

        void setTier(uint64_t h2, uint8_t tier) {
            Slot* s = probe(h2);
            if (s) s->tier = tier;
        }

        void eraseKey(uint64_t h2) {
            size_t idx = h2 & mask_;
            while (true) {
                if (slots_[idx].h2 == h2) {
                    size_t next = (idx + 1) & mask_;
                    while (slots_[next].h2 != 0) {
                        size_t ideal = slots_[next].h2 & mask_;
                        size_t dist_next = (next - ideal) & mask_;
                        size_t dist_idx = (next - idx) & mask_;
                        if (dist_next >= dist_idx) {
                            slots_[idx] = slots_[next];
                            idx = next;
                        }
                        next = (next + 1) & mask_;
                    }
                    slots_[idx] = {};
                    return;
                }
                if (slots_[idx].h2 == 0) return;
                idx = (idx + 1) & mask_;
            }
        }

        void initKey(uint64_t h2, HashPair hp, uint8_t tier) {
            Slot* s = probeOrInsert(h2);
            s->hashes = hp;
            s->state = augPack(AUG_REMARC_MAX, 0);
            s->lastEpoch = epoch_;
            s->avgGap = 0;
            s->tier = tier;
            pushToHeap(h2);
        }

        uint32_t epoch() const { return epoch_; }
        void advanceEpoch() { epoch_++; }

        uint8_t demoteCheck(uint64_t h2, uint8_t demoteThresh) {
            Slot* s = probe(h2);
            if (!s) return 0;
            uint32_t gap = epoch_ - s->lastEpoch;
            uint8_t r = augGetR(s->state), f = augGetF(s->state);
            for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
            if (demoteThresh > 0 && (uint16_t)r + f <= demoteThresh) {
                s->lastEpoch = epoch_;
                s->state = augPack(augDecayR(augGetR(s->state)), f);
                return 1;
            }
            s->lastEpoch = epoch_;
            pushToHeap(h2);
            return 0;
        }

        double confidence() const {
            double cv = (popMeanGap_ > 1.0)
                ? std::sqrt(std::max(0.0, popM2Gap_)) / popMeanGap_
                : 1.0;
            return std::max(0.0, 1.0 - cv * 3.0) *
                   std::max(0.0, 1.0 - regretEMA_);
        }
    };

    // =================================================================
    //  AugAdaptStore: ARC cache with AugAdaptScorer
    // =================================================================
    //
    //  Uses CMap<Value> for storage + FlatList for ARC structure +
    //  AugAdaptScorer for eviction decisions. Same public interface as
    //  ConcurrentARC so it's a drop-in Store replacement.

    template <class Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class AugAdaptStore {
    public:
        using EvictionCallback = std::function<void(const Value&)>;

    private:
        enum Tier : uint8_t { NONE = 0, T1 = 1, T2 = 2, B1 = 3, B2 = 4 };
        static constexpr uint8_t demoteThresh_ = 4;

        CMap<Value> store_;
        FlatList t1_, t2_, b1_, b2_;
        AugAdaptScorer scorer_;
        SpinLock lock_;
        PromoteBuf promoteBuf_;
        size_t capacity_;
        size_t p_;
        EvictionCallback evictionCallback_ = [](const Value&) {};

        bool evictAndGhost(Tier targetTier, Tier ghostTier) {
            HashPair victimHashes = scorer_.selectVictim(targetTier);
            uint64_t victimH2 = victimHashes.h2;
            bool usedHeap = (victimH2 != 0);

            if (!usedHeap) {
                auto& l = (targetTier == T1) ? t1_ : t2_;
                if (l.empty()) return false;
                victimHashes = l.back();
                victimH2 = victimHashes.h2;
            }

            auto result = store_.FindAndEraseByHash(victimHashes);
            if (result.err != NO_ERR) return false;

            if (targetTier == T1) t1_.erase(victimH2);
            else t2_.erase(victimH2);

            if (ghostTier != NONE) {
                auto& gl = (ghostTier == B1) ? b1_ : b2_;
                gl.push_front(victimHashes);
                scorer_.setTier(victimH2, ghostTier);
            } else {
                scorer_.eraseKey(victimH2);
            }
            if (result.value) evictionCallback_(*result.value);
            return true;
        }

        bool replaceLocked(HashPair hp) {
            bool evictT1 = !t1_.empty() && (t1_.size() > p_ ||
                (b2_.contains(hp.h2) && t1_.size() == p_));
            if (evictT1 && !t1_.empty()) {
                return evictAndGhost(T1, B1);
            } else if (!t2_.empty()) {
                return evictAndGhost(T2, B2);
            }
            return false;
        }

        bool evictLocked() {
            if (t1_.size() + b1_.size() >= capacity_) {
                if (t1_.size() < capacity_ && !b1_.empty()) {
                    uint64_t h2 = b1_.back().h2;
                    b1_.pop_back();
                    scorer_.eraseKey(h2);
                } else if (!t1_.empty()) {
                    if (!evictAndGhost(T1, NONE)) return false;
                }
            }
            if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * capacity_) {
                if (!b2_.empty()) {
                    uint64_t h2 = b2_.back().h2;
                    b2_.pop_back();
                    scorer_.eraseKey(h2);
                } else if (!t2_.empty()) {
                    if (!evictAndGhost(T2, NONE)) return false;
                }
            }
            return true;
        }

    public:
        AugAdaptStore(size_t cap, CMapAllocFn af = CMapDefaultAlloc, CMapFreeFn ff = CMapDefaultFree)
            : store_(cap, af, ff), t1_(2 * cap), t2_(2 * cap), b1_(2 * cap), b2_(2 * cap),
              scorer_(cap), capacity_(cap), p_(0) {}

        void SetEvictionCallback(EvictionCallback cb) { evictionCallback_ = cb; }

        uint8_t GetDesire(uint64_t h2) const {
            size_t idx = h2 & scorer_.mask_;
            while (true) {
                const auto& s = scorer_.slots_[idx];
                if (s.h2 == h2) return augGetR(s.state);
                if (s.h2 == 0) return 0;
                idx = (idx + 1) & scorer_.mask_;
            }
        }

        std::optional<Value> Find(const std::string& key) {
            auto val = store_.Find(key);
            if (!val) return std::nullopt;

            HashPair hashes = HashKey(key);
            if (!promoteBuf_.enqueue(hashes)) {
                if (lock_.try_lock()) {
                    promoteBuf_.drain(t1_, t2_);
                    lock_.unlock();
                }
            }
            return val;
        }

        template <typename Fn>
        Error UpdateInPlace(const std::string& key, Fn&& fn) {
            return store_.UpdateInPlace(key, std::forward<Fn>(fn));
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) {
            return store_.UpdateInPlaceByHash(hashes, std::forward<Fn>(fn));
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drain(t1_, t2_);
            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            t1_.erase(h2);
            t2_.erase(h2);
            b1_.erase(h2);
            b2_.erase(h2);
            scorer_.eraseKey(h2);
            return store_.FindAndErase(key);
        }

        typename CMap<Value>::FindAndEraseResult EraseByHash(const HashPair& hashes) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drain(t1_, t2_);
            uint64_t h2 = hashes.h2;
            t1_.erase(h2);
            t2_.erase(h2);
            b1_.erase(h2);
            b2_.erase(h2);
            scorer_.eraseKey(h2);
            return store_.FindAndEraseByHash(hashes);
        }

        Error Set(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drain(t1_, t2_);
            scorer_.advanceEpoch();

            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;

            AugAdaptScorer* sc = &scorer_;
            (void)sc;

            bool inT1 = t1_.contains(h2);
            bool inT2 = t2_.contains(h2);
            bool inB1 = b1_.contains(h2);
            bool inB2 = b2_.contains(h2);

            if (inT1 || inT2) {
                uint8_t tier = inT1 ? T1 : T2;
                scorer_.touchKey(h2, hashes, tier);

                if (inT1) {
                    t1_.erase(h2);
                    t2_.push_front(hashes);
                    scorer_.setTier(h2, T2);
                } else {
                    if (scorer_.demoteCheck(h2, demoteThresh_)) {
                        t2_.erase(h2);
                        t1_.push_front(hashes);
                        scorer_.setTier(h2, T1);
                    } else {
                        t2_.splice_front(h2);
                    }
                }
                return store_.Set(key, val).err;
            }

            Tier ghostTier = NONE;
            if (inB1) {
                scorer_.onGhostHit(h2);
                size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
                p_ = std::min(capacity_, p_ + d);
                b1_.erase(h2);
                ghostTier = B1;
            } else if (inB2) {
                scorer_.onGhostHit(h2);
                size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
                size_t dec = std::max(d, (size_t)1);
                p_ = (p_ >= dec) ? p_ - dec : 0;
                b2_.erase(h2);
                ghostTier = B2;
            }

            if (ghostTier != NONE) {
                if (!evictLocked()) return ABANDONED_SET;
                if (!replaceLocked(hashes)) return ABANDONED_SET;
                scorer_.touchKey(h2, hashes, T2);
                t2_.push_front(hashes);
                scorer_.setTier(h2, T2);
            } else {
                if (!evictLocked()) return ABANDONED_SET;
                scorer_.initKey(h2, hashes, T1);
                t1_.push_front(hashes);
            }

            return store_.Set(key, val).err;
        }
    };

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class ConcurrentLRU {
        CMap<Value> store_;
        FlatList lru_;
        SpinLock lock_;
        PromoteBuf promoteBuf_;
        size_t capacity_;
        using EvictionCallback = std::function<void(const Value&)>;
        EvictionCallback evictionCallback_;

        bool evictTailLocked() {
            if (lru_.empty()) return false;
            HashPair victim = lru_.back();
            auto result = store_.FindAndEraseByHash(victim);
            if (result.err != NO_ERR) return false;
            lru_.pop_back();
            if (result.value && evictionCallback_) evictionCallback_(*result.value);
            return true;
        }

    public:
        ConcurrentLRU(size_t cap,
                      CMapAllocFn af = CMapDefaultAlloc,
                      CMapFreeFn ff = CMapDefaultFree)
            : store_(cap, af, ff), lru_(2 * cap), capacity_(cap) {}

        void SetEvictionCallback(EvictionCallback cb) {
            evictionCallback_ = std::move(cb);
        }

        bool ForceEvictOne() {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drainSingle(lru_);
            return evictTailLocked();
        }

        uint8_t GetDesire(uint64_t) const { return 0; }

        std::optional<Value> Find(const std::string& key) {
            auto val = store_.Find(key);
            if (!val) return std::nullopt;
            HashPair hashes = HashKey(key);
            if (!promoteBuf_.enqueue(hashes)) {
                if (lock_.try_lock()) {
                    promoteBuf_.drainSingle(lru_);
                    lock_.unlock();
                }
            }
            return val;
        }

        template <typename Fn>
        Error UpdateInPlace(const std::string& key, Fn&& fn) {
            return store_.UpdateInPlace(key, std::forward<Fn>(fn));
        }

        template <typename Fn>
        Error UpdateInPlaceByHash(const HashPair& hashes, Fn&& fn) {
            return store_.UpdateInPlaceByHash(hashes, std::forward<Fn>(fn));
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drainSingle(lru_);
            HashPair hashes = HashKey(key);
            lru_.erase(hashes.h2);
            return store_.FindAndErase(std::string_view(key));
        }

        auto EraseByHash(const HashPair& hashes) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drainSingle(lru_);
            lru_.erase(hashes.h2);
            return store_.FindAndEraseByHash(hashes);
        }

        Error Set(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drainSingle(lru_);
            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            CMapSetResult result = store_.Set(key, val);
            if (result.err != NO_ERR) return result.err;

            if (!result.inserted) {
                lru_.splice_front(h2);
                return NO_ERR;
            }

            while (lru_.size() >= capacity_) {
                if (!evictTailLocked()) break;
            }
            lru_.push_front(hashes);
            return NO_ERR;
        }

        Error EvictAndSet(const std::string& key, const Value& val) {
            std::lock_guard<SpinLock> guard(lock_);
            promoteBuf_.drainSingle(lru_);

            if (!evictTailLocked()) return ABANDONED_SET;

            HashPair hashes = HashKey(key);
            uint64_t h2 = hashes.h2;
            CMapSetResult result = store_.Set(key, val);
            if (result.err != NO_ERR) return result.err;

            if (!result.inserted) {
                lru_.splice_front(h2);
                return NO_ERR;
            }

            while (lru_.size() >= capacity_) {
                if (!evictTailLocked()) break;
            }
            lru_.push_front(hashes);
            return NO_ERR;
        }
    };

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class RawCMapStore {
        CMap<Value> store_;
    public:
        RawCMapStore(size_t cap,
                     CMapAllocFn af = CMapDefaultAlloc,
                     CMapFreeFn ff = CMapDefaultFree)
            : store_(cap, af, ff) {}

        void SetEvictionCallback(auto) {}

        uint8_t GetDesire(uint64_t) const { return 0; }

        std::optional<Value> Find(const std::string& key) {
            return store_.Find(std::string_view(key));
        }

        Error UpdateInPlace(const std::string& key, auto&& fn) {
            return store_.UpdateInPlace(std::string_view(key), std::forward<decltype(fn)>(fn));
        }

        Error UpdateInPlaceByHash(const HashPair& hashes, auto&& fn) {
            return store_.UpdateInPlaceByHash(hashes, std::forward<decltype(fn)>(fn));
        }

        typename CMap<Value>::FindAndEraseResult Erase(const std::string& key) {
            return store_.FindAndErase(std::string_view(key));
        }

        auto EraseByHash(const HashPair& hashes) {
            return store_.FindAndEraseByHash(hashes);
        }

        Error Set(const std::string& key, const Value& val) {
            auto r = store_.Set(std::string_view(key), val);
            return r.err;
        }
    };

} // namespace NuAtlas
