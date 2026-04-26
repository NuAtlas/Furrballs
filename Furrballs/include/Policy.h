#pragma once
#include "Remarc.h"

namespace NuAtlas {

    // --- REMARC Policy: E = P(A, B) ---
    // Composes two atoms under a packed-nibble storage layout with standard
    // lookup-table scoring.  The projection (eviction / migration formula)
    // is defined by EvictLookup / MigrateLookup; the atoms define per-access
    // boost / decay behavior.  Changing either changes the policy.

    template<typename AtomA, typename AtomB>
    struct RemarcPolicy {
        using Config = RemarcConfig;
        using PerKeyState = uint8_t;

        static constexpr bool HasPerKeyState = true;
        static constexpr bool HasMigration   = true;
        static constexpr bool HasScanner     = true;
        static constexpr bool HasDesire      = true;

        static Config MakeConfig(const RemarcConfig& rc) noexcept { return rc; }

        static uint8_t InitialState() noexcept {
            return PackTempCtrl(AtomA::Initial, AtomB::Initial);
        }

        static uint8_t OnLocalAccess(uint8_t tc, const Config& cfg) noexcept {
            uint8_t a = AtomA::Promote(UnpackSLocal(tc), cfg.AlphaLocal);
            uint8_t b = AtomB::Demote(UnpackSRemote(tc), cfg.AlphaLocal);
            return PackTempCtrl(a, b);
        }

        static uint8_t OnRemoteAccess(uint8_t tc, const Config& cfg) noexcept {
            uint8_t a = AtomA::Demote(UnpackSLocal(tc), cfg.AlphaRemote);
            uint8_t b = AtomB::Promote(UnpackSRemote(tc), cfg.AlphaRemote);
            return PackTempCtrl(a, b);
        }

        static void TimeDecayKey(uint8_t& tc, const Config& cfg) noexcept {
            uint8_t a = AtomA::TimeDecay(UnpackSLocal(tc), cfg.TimeDecayNum, cfg.TimeDecayDen);
            uint8_t b = AtomB::TimeDecay(UnpackSRemote(tc), cfg.TimeDecayNum, cfg.TimeDecayDen);
            tc = PackTempCtrl(a, b);
        }

        static uint8_t EvictScore(uint8_t tc) noexcept { return EvictLookup[tc]; }
        static uint8_t MigrateScore(uint8_t tc) noexcept { return MigrateLookup[tc]; }
        static bool ShouldHotNode(uint8_t tc, const Config& cfg) noexcept {
            return UnpackSRemote(tc) >= cfg.HotNodeProb;
        }

        static RemarcScanResult ScanBatch(const uint8_t* d, size_t o, size_t n,
                                          const Config& c) noexcept {
            return RemarcScanBatch(d, o, n, c);
        }
        static float EPage(uint32_t sum, size_t n) noexcept {
            return RemarcComputeEPage(sum, n);
        }
    };

    // --- ARC Policy: no per-key state, no migration, no scanner ---

    struct ArcPolicy {
        struct Config {};
        using PerKeyState = uint8_t;

        static constexpr bool HasPerKeyState = false;
        static constexpr bool HasMigration   = false;
        static constexpr bool HasScanner     = false;
        static constexpr bool HasDesire      = false;

        static Config MakeConfig(const RemarcConfig&) noexcept { return {}; }
        static uint8_t InitialState() noexcept { return 0; }
    };

    // --- Standard 2-node REMARC: s_local (recency) + s_remote (frequency) ---

    using StandardRemarc = RemarcPolicy<AtomSLocal, AtomSRemote>;

} // namespace NuAtlas
