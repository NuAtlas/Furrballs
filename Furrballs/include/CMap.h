#pragma once
#include "Error.h"
#include <bit>
#include <atomic>
#include <emmintrin.h>
#include <xxh3.h>
#include <xxhash.h>
#include <optional>
#include <cstring>
#include <cassert>
#include <new>
#include <list>
#include <string>
#include <algorithm>
#include <functional>
#include <unordered_map>

namespace NuAtlas {

    struct HashPair {
        uint64_t h1, h2;
    };

    class SpinLock {
        std::atomic<bool> locked_{false};
    public:
        void lock() noexcept {
            while (locked_.exchange(true, std::memory_order_acquire)) {
                _mm_pause();
            }
        }
        bool try_lock() noexcept {
            return !locked_.exchange(true, std::memory_order_acquire);
        }
        void unlock() noexcept {
            locked_.store(false, std::memory_order_release);
        }
    };

    class ArcList {
        std::list<HashPair> list_;
        std::unordered_map<uint64_t, std::list<HashPair>::iterator> index_;
    public:
        bool contains(uint64_t h2) const {
            return index_.find(h2) != index_.end();
        }
        void push_front(HashPair hashes) {
            list_.push_front(hashes);
            index_[hashes.h2] = list_.begin();
        }
        void pop_back() {
            if (!list_.empty()) {
                index_.erase(list_.back().h2);
                list_.pop_back();
            }
        }
        void erase(uint64_t h2) {
            auto it = index_.find(h2);
            if (it != index_.end()) {
                list_.erase(it->second);
                index_.erase(it);
            }
        }
        void splice_front(uint64_t h2) {
            auto it = index_.find(h2);
            if (it != index_.end()) {
                list_.splice(list_.begin(), list_, it->second);
            }
        }
        HashPair back() const { return list_.back(); }
        size_t size() const { return list_.size(); }
        bool empty() const { return list_.empty(); }
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
    //       void drainAll(ArcList& t1, ArcList& t2) {
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

        void drain(ArcList& t1, ArcList& t2) {
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

            fn(targetSlot.value);

            targetSlot.seq.store(expected + 2, std::memory_order_release);
            return NO_ERR;
        }

        Error Erase(std::string_view key) noexcept {
            return FindAndErase(key).err;
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
        ArcList t1_, t2_, b1_, b2_;
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
            : store_(cap, af, ff), capacity_(cap), p_(0) {}

        void SetEvictionCallback(EvictionCallback cb) { evictionCallback_ = cb; }

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
    };

} // namespace NuAtlas
