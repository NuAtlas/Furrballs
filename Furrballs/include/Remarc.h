#pragma once
#include <cstdint>
#include <cstddef>
#include <atomic>
#include <array>
#include <utility>
#include <immintrin.h>

namespace NuAtlas
{
    struct HashPair {
        uint64_t h1, h2;
        bool operator==(const HashPair& o) const noexcept { return h1 == o.h1 && h2 == o.h2; }
    };

    class SpinLock {
        std::atomic<bool> locked_{false};
    public:
        void lock() noexcept {
            for (;;) {
                while (locked_.load(std::memory_order_relaxed)) {
                    _mm_pause();
                }
                if (!locked_.exchange(true, std::memory_order_acquire)) return;
            }
        }
        bool try_lock() noexcept {
            return !locked_.exchange(true, std::memory_order_acquire);
        }
        void unlock() noexcept {
            locked_.store(false, std::memory_order_release);
        }
    };

    class alignas(64) MCSLock {
        struct Node {
            std::atomic<bool> waiting{true};
            Node* next{nullptr};
        };

        std::atomic<Node*> tail_{nullptr};

    public:
        void lock() noexcept {
            Node* me = &localNode_;
            me->waiting.store(true, std::memory_order_relaxed);
            me->next = nullptr;

            Node* prev = tail_.exchange(me, std::memory_order_acq_rel);
            if (!prev) return;

            prev->next = me;
            while (me->waiting.load(std::memory_order_acquire)) {
                _mm_pause();
            }
        }

        void unlock() noexcept {
            Node* me = &localNode_;
            if (!me->next) {
                Node* expected = me;
                if (tail_.compare_exchange_strong(expected, nullptr,
                        std::memory_order_release, std::memory_order_relaxed)) {
                    return;
                }
                while (!me->next) {
                    _mm_pause();
                }
            }
            me->next->waiting.store(false, std::memory_order_release);
            me->next = nullptr;
        }

    private:
        static thread_local Node localNode_;
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
        float StaticEvictThresh = -1.0f;
        uint8_t MaxDeadAge = 8;
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

    inline uint8_t RemarcUpdateLocal(uint8_t tc, const RemarcConfig& cfg) noexcept {
        uint8_t s_local = UnpackSLocal(tc);
        uint8_t s_remote = UnpackSRemote(tc);
        s_local = RemarcBoost(s_local, cfg.AlphaLocal);
        s_remote = RemarcDecay(s_remote, cfg.AlphaLocal);
        return PackTempCtrl(s_local, s_remote);
    }

    inline uint8_t RemarcUpdateRemote(uint8_t tc, const RemarcConfig& cfg) noexcept {
        uint8_t s_local = UnpackSLocal(tc);
        uint8_t s_remote = UnpackSRemote(tc);
        s_remote = RemarcBoost(s_remote, cfg.AlphaRemote);
        s_local = RemarcDecay(s_local, cfg.AlphaRemote);
        return PackTempCtrl(s_local, s_remote);
    }

    inline bool RemarcShouldUpdateHotNode(uint8_t tc, const RemarcConfig& cfg) noexcept {
        return UnpackSRemote(tc) >= cfg.HotNodeProb;
    }

    struct RemarcScanResult {
        uint32_t evictMask;
        uint32_t migrateMask;
        uint32_t ePageNumSum;
    };

    namespace RemarcDetail {
        inline uint32_t HSumEpu16(__m256i v) noexcept {
            __m128i lo = _mm256_castsi256_si128(v);
            __m128i hi = _mm256_extracti128_si256(v, 1);
            __m128i s = _mm_hadd_epi16(lo, hi);
            __m128i z = _mm_setzero_si128();
            s = _mm_hadd_epi16(s, z);
            s = _mm_hadd_epi16(s, z);
            s = _mm_hadd_epi16(s, z);
            return static_cast<uint32_t>(_mm_extract_epi16(s, 0));
        }

        inline uint32_t CompressCmpMask(__m256i cmp) noexcept {
            uint32_t raw = static_cast<uint32_t>(_mm256_movemask_epi8(cmp));
            return _pext_u32(raw, 0x55555555u);
        }
    }

    inline RemarcScanResult RemarcScanBatch(
        const uint8_t* tempCtrl, size_t offset, size_t count,
        const RemarcConfig& cfg) noexcept
    {
        RemarcScanResult result = {};

        size_t batchLen = count - offset;
        if (batchLen > 32) batchLen = 32;

        if (batchLen <= 8) {
            uint16_t te16 = static_cast<uint16_t>(cfg.ThetaEvict) * 30;
            for (size_t i = 0; i < batchLen; i++) {
                uint8_t tc = tempCtrl[offset + i];
                uint8_t sl = UnpackSLocal(tc);
                uint8_t sr = UnpackSRemote(tc);
                uint16_t e_num = static_cast<uint16_t>((15 - sl) * (30 - sl - sr));
                if (e_num > te16)
                    result.evictMask |= (1u << i);
                if (RemarcMigrateScore(tc) > cfg.ThetaMigrate)
                    result.migrateMask |= (1u << i);
                result.ePageNumSum += e_num;
            }
            return result;
        }

        __m256i tc = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tempCtrl + offset));
        __m256i m0f = _mm256_set1_epi8(0x0F);
        __m256i sl8 = _mm256_and_si256(tc, m0f);
        __m256i sr8 = _mm256_and_si256(_mm256_srli_epi16(tc, 4), m0f);

        __m256i sl_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(sl8));
        __m256i sl_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(sl8, 1));
        __m256i sr_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(sr8));
        __m256i sr_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(sr8, 1));

        __m256i fifteen = _mm256_set1_epi16(15);
        __m256i thirty = _mm256_set1_epi16(30);
        __m256i te = _mm256_set1_epi16(static_cast<short>(cfg.ThetaEvict * 30));

        __m256i R_lo = _mm256_sub_epi16(fifteen, sl_lo);
        __m256i F_lo = _mm256_add_epi16(sl_lo, sr_lo);
        __m256i Enum_lo = _mm256_mullo_epi16(R_lo, _mm256_sub_epi16(thirty, F_lo));
        __m256i evict_lo = _mm256_cmpgt_epi16(Enum_lo, te);

        __m256i R_hi = _mm256_sub_epi16(fifteen, sl_hi);
        __m256i F_hi = _mm256_add_epi16(sl_hi, sr_hi);
        __m256i Enum_hi = _mm256_mullo_epi16(R_hi, _mm256_sub_epi16(thirty, F_hi));
        __m256i evict_hi = _mm256_cmpgt_epi16(Enum_hi, te);

        result.evictMask = RemarcDetail::CompressCmpMask(evict_lo) |
                           (RemarcDetail::CompressCmpMask(evict_hi) << 16);
        result.ePageNumSum = RemarcDetail::HSumEpu16(Enum_lo) +
                             RemarcDetail::HSumEpu16(Enum_hi);

        for (size_t i = 0; i < batchLen; i++) {
            if (MigrateLookup[tempCtrl[offset + i]] > cfg.ThetaMigrate)
                result.migrateMask |= (1u << i);
        }

        if (batchLen < 32) {
            uint32_t valid = (1u << batchLen) - 1;
            result.evictMask &= valid;
            result.migrateMask &= valid;
        }

        return result;
    }

    inline float RemarcComputeEPage(uint32_t ePageNumSum, size_t keyCount) noexcept {
        if (keyCount == 0) return 0.0f;
        return static_cast<float>(ePageNumSum) / (30.0f * static_cast<float>(keyCount));
    }

    enum class PageTier : uint8_t {
        Hot = 0,
        Cold = 1,
        Empty = 2,
        Freeze = 3,
        Dead = 4,
        Staging = 5
    };

    // --- REMARC Atom Framework ---
    // Each atom is one dimension of per-key state in the decomposition E = P(A₁, ..., Aₙ).
    // Stateless: behavior only. State lives in Page::TempCtrl, interpreted by the Policy.

    struct AtomSLocal {
        static constexpr uint8_t Initial = REMARC_MAX;
        static uint8_t Promote(uint8_t s, uint8_t alpha) noexcept { return RemarcBoost(s, alpha); }
        static uint8_t Demote(uint8_t s, uint8_t alpha) noexcept { return RemarcDecay(s, alpha); }
        static uint8_t TimeDecay(uint8_t s, uint8_t num, uint8_t den) noexcept { return RemarcTimeDecay(s, num, den); }
    };

    struct AtomSRemote {
        static constexpr uint8_t Initial = 0;
        static uint8_t Promote(uint8_t s, uint8_t alpha) noexcept { return RemarcBoost(s, alpha); }
        static uint8_t Demote(uint8_t s, uint8_t alpha) noexcept { return RemarcDecay(s, alpha); }
        static uint8_t TimeDecay(uint8_t s, uint8_t num, uint8_t den) noexcept { return RemarcTimeDecay(s, num, den); }
    };

} // namespace NuAtlas
