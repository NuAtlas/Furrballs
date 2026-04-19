#pragma once
#include "Concept.h"
#include "MemoryManager.h"
#include "Error.h"
#include <bit>
#include <emmintrin.h>
#include <xxh3.h>
#include <xxhash.h>

namespace NuAtlas {

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
    struct HashPair {
        uint64_t h1, h2;
    };

    static HashPair HashKey(std::string_view key) {
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
        static constexpr float kMaxLoadFactor = 0.875;
        static constexpr unsigned int kGroupSize = 16;

        struct alignas(64) Slot {
            static constexpr bool kNeedsSplit = (16 + sizeof(Value)) > 64;

            std::atomic<uint8_t> seq;
            uint8_t padding[7];
            uint64_t fingerprint;

            alignas(kNeedsSplit ? 64 : alignof(Value)) Value value;
        };

        const uint8_t* Ctrl() const { return SlotCtrlPair.second; }
        Slot* Slots() { return SlotCtrlPair.first; }
        const Slot* Slots() const { return SlotCtrlPair.first; }

        CoAllocPair<Slot, uint8_t> SlotCtrlPair;
        size_t numGroups = 0;
        size_t slotCount = 0;

      public:
        // Capacity will be rounded to a power of 2
        CMap(size_t capacity) {
            // Round up to power of 2
            size_t allocCap = 1;
            while (allocCap < capacity) allocCap <<= 1;

            numGroups = allocCap / kGroupSize;
            slotCount = allocCap;

            SlotCtrlPair = CoAllocate<Slot, uint8_t>(slotCount, slotCount + kGroupSize);

            // Initialize all control bytes to empty
            memset(SlotCtrlPair.second, 0x80, slotCount + kGroupSize);

            // Set sentinel bytes at the end
            for (size_t i = slotCount; i < slotCount + kGroupSize; i++) { SlotCtrlPair.second[i] = 0xFF; }
        }

        ~CMap() {
            // TODO: Destroy all the values
            // for (size_t i = slotCount; i < slotCount + kGroupSize; i++)
            // {

            // }

            SlotCtrlPair.dealloc();
        }

        int MatchCtrlMask(__m128i ctrl_vec, uint8_t target) const noexcept {
            return _mm_movemask_epi8(_mm_cmpeq_epi8(ctrl_vec, _mm_set1_epi8(target)));
        }

        template <bool FindDeleted = false> ProbeResult Probe(const HashPair& hashes) const noexcept {
            ProbeResult result;
            auto group = hashes.h1 & (numGroups - 1);
            auto h2_short = static_cast<uint8_t>(hashes.h2 >> 57);

            for (size_t probeCount = 0; probeCount < numGroups; ++probeCount) {
                __m128i ctrl_vec = _mm_loadu_si128(reinterpret_cast<const __m128i*>(Ctrl() + group * kGroupSize));
                int matchMask = MatchCtrlMask(ctrl_vec, h2_short);
                while (matchMask != 0) {
                    int i = std::countr_zero(static_cast<unsigned>(matchMask));
                    matchMask &= matchMask - 1;
                    size_t slotIdx = group * kGroupSize + static_cast<size_t>(i);
                    if (Slots()[slotIdx].fingerprint == hashes.h2) {
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

                group = (group + 1) & (numGroups - 1);
            }
            return result; 
        }

        std::optional<Value> Find(std::string_view key) const noexcept {
            HashPair pair = HashKey(key);
            ProbeResult result = Probe<false>(pair);
            if (result.matchSlot == SIZE_MAX) return std::nullopt;
            const Slot& slot = Slots()[result.matchSlot];
            Value copy;
            uint8_t s1;
            do {
                s1 = slot.seq.load(std::memory_order_acquire);
                if (s1 & 1) { _mm_pause(); continue; }
                std::memcpy(&copy, &slot.value, sizeof(Value));
            } while (s1 != slot.seq.load(std::memory_order_acquire));
            return copy;
        }

    };
} // namespace NuAtlas
