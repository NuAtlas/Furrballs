#pragma once
#include <cstdint>
#include <cstddef>
#include <atomic>
#include <array>
#include <utility>
#include <emmintrin.h>
#include <tmmintrin.h>

namespace NuAtlas
{
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

    constexpr uint8_t REMARC_MAX = 15;

    struct RemarcConfig {
        uint8_t AlphaLocal = 2;
        uint8_t AlphaRemote = 1;
        uint8_t ThetaEvict = 12;
        uint8_t ThetaMigrate = 10;
        uint8_t HotNodeProb = 13;
        uint8_t TimeDecayNum = 7;
        uint8_t TimeDecayDen = 8;
    };

    namespace RemarcDetail {
        inline constexpr uint8_t ComputeEvict(uint8_t s_local, uint8_t s_remote) noexcept {
            uint16_t r = static_cast<uint16_t>(s_local);
            uint16_t f = static_cast<uint16_t>(s_local + s_remote);
            uint16_t e = (REMARC_MAX - r) * (2 * REMARC_MAX - f);
            return static_cast<uint8_t>(e / (REMARC_MAX * 2));
        }

        inline constexpr uint8_t ComputeMigrate(uint8_t s_local, uint8_t s_remote) noexcept {
            if (s_remote == 0) return 0;
            uint16_t r = static_cast<uint16_t>(s_local);
            uint16_t f = static_cast<uint16_t>(s_local + s_remote);
            uint16_t l_num = static_cast<uint16_t>(s_remote);
            uint16_t l_den = f > 0 ? f : 1;
            uint16_t m = (2 * REMARC_MAX - f) * l_num * (REMARC_MAX - r) / l_den;
            return static_cast<uint8_t>(m / (REMARC_MAX * 2 * REMARC_MAX));
        }

        template<size_t... Is>
        constexpr auto MakeEvictTable(std::index_sequence<Is...>) noexcept {
            return std::array<uint8_t, 256>{ComputeEvict(Is & 0xF, Is >> 4)...};
        }

        template<size_t... Is>
        constexpr auto MakeMigrateTable(std::index_sequence<Is...>) noexcept {
            return std::array<uint8_t, 256>{ComputeMigrate(Is & 0xF, Is >> 4)...};
        }
    }

    inline constexpr auto EvictLookup = RemarcDetail::MakeEvictTable(std::make_index_sequence<256>{});
    inline constexpr auto MigrateLookup = RemarcDetail::MakeMigrateTable(std::make_index_sequence<256>{});

    inline constexpr uint8_t PackTempCtrl(uint8_t s_local, uint8_t s_remote) noexcept {
        return static_cast<uint8_t>((s_remote << 4) | s_local);
    }

    inline constexpr uint8_t UnpackSLocal(uint8_t tc) noexcept { return tc & 0xF; }
    inline constexpr uint8_t UnpackSRemote(uint8_t tc) noexcept { return tc >> 4; }

    inline uint8_t RemarcBoost(uint8_t current, uint8_t alpha) noexcept {
        uint16_t delta_num = static_cast<uint16_t>(alpha) * (REMARC_MAX - current);
        uint8_t delta = static_cast<uint8_t>((delta_num + REMARC_MAX / 2) / REMARC_MAX);
        return current + delta;
    }

    inline uint8_t RemarcDecay(uint8_t current, uint8_t alpha) noexcept {
        uint16_t decay_num = static_cast<uint16_t>(current) * (REMARC_MAX - alpha);
        return static_cast<uint8_t>((decay_num + REMARC_MAX / 2) / REMARC_MAX);
    }

    inline uint8_t RemarcTimeDecay(uint8_t current, uint8_t num, uint8_t den) noexcept {
        uint16_t v = static_cast<uint16_t>(current) * num;
        return static_cast<uint8_t>((v + den / 2) / den);
    }

    inline uint8_t RemarcEvictScore(uint8_t tc) noexcept {
        return EvictLookup[tc];
    }

    inline uint8_t RemarcMigrateScore(uint8_t tc) noexcept {
        return MigrateLookup[tc];
    }

    enum class PageTier : uint8_t {
        Hot = 0,
        Cold = 1,
        Empty = 2,
        Freeze = 3
    };

} // namespace NuAtlas
