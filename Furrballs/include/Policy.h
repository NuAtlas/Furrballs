#pragma once
#include "Remarc.h"
#include <type_traits>

namespace NuAtlas {

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class ConcurrentARC;

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class AugAdaptStore;

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class NativeRemarcStore;

    // --- Policy Concept ---
    //
    // A cache Policy IS the cache. It owns the key store, manages admission
    // and eviction, and provides scoring. FurrBall<Policy> delegates all cache
    // decisions to Policy.
    //
    // Required interface:
    //   template<typename Value> using Store = ...;
    //   using Config       = ...;
    //   static Config MakeConfig(const RemarcConfig&);
    //   static uint8_t InitialState();
    //
    // Optional (constexpr false = disabled):
    //   static constexpr bool HasPerKeyState;
    //   static constexpr bool HasMigration;
    //   static constexpr bool HasScanner;
    //   static constexpr bool HasDesire;
    //   static constexpr bool HasStoreEviction; // store handles eviction internally
    //   static uint8_t OnLocalAccess/OnRemoteAccess/EvictScore/etc.

    // --- ARC Policy: standard ARC with no per-key state ---

    struct ArcPolicy {
        template<typename Value> using Store = ConcurrentARC<Value>;
        using Config = struct {};

        static constexpr bool HasPerKeyState = false;
        static constexpr bool HasMigration   = false;
        static constexpr bool HasScanner     = false;
        static constexpr bool HasDesire      = false;
        static constexpr bool HasStoreEviction = false;

        static Config MakeConfig(const RemarcConfig&) noexcept { return {}; }
        static uint8_t InitialState() noexcept { return 0; }
    };

    // --- REMARC Policy: E = P(A, B) ---
    // Concurrent ARC key store + REMARC per-key scoring + SIMD scanning.

    template<typename AtomA, typename AtomB>
    struct RemarcPolicy {
        template<typename Value> using Store = ConcurrentARC<Value>;
        using Config = RemarcConfig;

        static constexpr bool HasPerKeyState = true;
        static constexpr bool HasMigration   = true;
        static constexpr bool HasScanner     = true;
        static constexpr bool HasDesire      = true;
        static constexpr bool HasStoreEviction = false;

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

    // --- Standard 2-node REMARC: s_local (recency) + s_remote (frequency) ---

    using StandardRemarc = RemarcPolicy<AtomSLocal, AtomSRemote>;

    // --- AUG-ADAPT: ARC + adaptive heap eviction + REMARC scoring ---

    struct AugAdaptPolicy {
        template<typename Value> using Store = AugAdaptStore<Value>;
        using Config = RemarcConfig;

        static constexpr bool HasPerKeyState = true;
        static constexpr bool HasMigration   = true;
        static constexpr bool HasScanner     = true;
        static constexpr bool HasDesire      = true;
        static constexpr bool HasStoreEviction = false;

        static Config MakeConfig(const RemarcConfig& rc) noexcept { return rc; }

        static uint8_t InitialState() noexcept {
            return PackTempCtrl(AtomSLocal::Initial, AtomSRemote::Initial);
        }

        static uint8_t OnLocalAccess(uint8_t tc, const Config& cfg) noexcept {
            uint8_t a = AtomSLocal::Promote(UnpackSLocal(tc), cfg.AlphaLocal);
            uint8_t b = AtomSRemote::Demote(UnpackSRemote(tc), cfg.AlphaLocal);
            return PackTempCtrl(a, b);
        }

        static uint8_t OnRemoteAccess(uint8_t tc, const Config& cfg) noexcept {
            uint8_t a = AtomSLocal::Demote(UnpackSLocal(tc), cfg.AlphaRemote);
            uint8_t b = AtomSRemote::Promote(UnpackSRemote(tc), cfg.AlphaRemote);
            return PackTempCtrl(a, b);
        }

        static void TimeDecayKey(uint8_t& tc, const Config& cfg) noexcept {
            uint8_t a = AtomSLocal::TimeDecay(UnpackSLocal(tc), cfg.TimeDecayNum, cfg.TimeDecayDen);
            uint8_t b = AtomSRemote::TimeDecay(UnpackSRemote(tc), cfg.TimeDecayNum, cfg.TimeDecayDen);
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

    // --- NATIVE-REMARc: scorer IS the cache, no ARC structure ---
    // Direct eviction by desire score. No T1/T2/B1/B2, no ghost lists,
    // no p-adaptation. Min-heap drives eviction order.

    struct NativeRemarcPolicy {
        template<typename Value> using Store = NativeRemarcStore<Value>;
        using Config = RemarcConfig;

        static constexpr bool HasPerKeyState = false;
        static constexpr bool HasMigration   = false;
        static constexpr bool HasScanner     = false;
        static constexpr bool HasDesire      = true;
        static constexpr bool HasStoreEviction = true;

        static Config MakeConfig(const RemarcConfig& rc) noexcept { return rc; }
        static uint8_t InitialState() noexcept { return 0; }
    };

} // namespace NuAtlas
