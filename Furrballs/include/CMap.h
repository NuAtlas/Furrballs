#pragma once
#include "Concept.h"
#include "MemoryManager.h"
#include <xxh3.h>
#include <xxhash.h>
#include <emmintrin.h>

namespace NuAtlas {
    enum class CMap_H2_State : uint8_t //<-- Exact width.
    {
        kEmpty = 0x80,
        kDeleted = 0xFE,
        kSentinel = 0xFF
    };

    // h2 is also used for fingerprint. To reduce colisions. We are reducing the probability, but we can't remove it.
    struct HashPair {
        uint64_t h1, h2;
    };
    HashPair HashKey(std::string_view key) {
        XXH128_hash_t h = XXH3_128bits(key.data(), key.size());
        return {h.high64, h.low64};
    }

    // Swiss table varient, For NUMA backed bins create in the node.
    // Furrballs currently allocates the Entire Node metadata in the node's memory and we must ensure that it stays that way or have a dynamic allocator.
    // TODO: re-add back the Key in the template, not needed for now.
    template <class Value>
        requires std::is_move_constructible_v<Value>
    class CMap {
    private:
        const float kMaxLoadFactor = 0.875;
        const unsigned int kGroupSize = 16;

        struct Slot {
            static constexpr bool kNeedsSplit = (16 + sizeof(Value)) > 64;
            
            std::atomic<uint8_t> seq;
            uint8_t padding[7];
            uint64_t fingerprint;
            
            alignas(kNeedsSplit ? 64 : alignof(Value)) Value value;
        };
        

        // Slot* slots; //This will be allocated on the caller's heap.
        // std::array<uint8_t, CMap::kGroupSize> ctrlBytes;

        CoAllocPair<Slot, uint8_t> SlotCtrlPair;
        size_t numGroups = 0;
        size_t slotCount = 0;
        
    public:
        // Capacity will be rounded to a power of 2
        CMap(size_t capacity)
        {
            // Round up to power of 2
            size_t allocCap = 1;
            while (allocCap < capacity)
                allocCap <<= 1;

            numGroups = allocCap / kGroupSize;
            slotCount = allocCap;

            SlotCtrlPair = CoAllocate<Slot, uint8_t>(slotCount, slotCount + kGroupSize);

            // Initialize all control bytes to empty
            memset(SlotCtrlPair.second, 0x80, slotCount + kGroupSize);

            // Set sentinel bytes at the end
            for (size_t i = slotCount; i < slotCount + kGroupSize; i++)
            {
                SlotCtrlPair.second[i] = 0xFF;
            }
        }
    };
} // namespace NuAtlas
