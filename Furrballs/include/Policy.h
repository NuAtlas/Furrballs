#pragma once
#include <type_traits>

namespace NuAtlas {

    template<typename Value>
        requires std::is_move_constructible_v<Value> && std::is_trivially_copyable_v<Value>
    class ConcurrentARC;

    // --- Policy Concept ---
    //
    // A cache Policy IS the cache. It owns the key store, manages admission
    // and eviction, and provides scoring. FurrBall<Policy> delegates all cache
    // decisions to Policy.
    //
    // Required interface:
    //   template<typename Value> using Store = ...;
    //   using Config       = ...;
    //   static Config MakeConfig();
    //   static uint8_t InitialState();
    //
    // Optional (constexpr false = disabled):
    //   static constexpr bool HasPerKeyState;
    //   static constexpr bool HasMigration;
    //   static constexpr bool HasScanner;
    //   static constexpr bool HasDesire;
    //   static constexpr bool HasStoreEviction;

    // --- ARC Policy: standard ARC with no per-key state ---

    struct ArcPolicy {
        template<typename Value> using Store = ConcurrentARC<Value>;
        using Config = struct {};

        static constexpr bool HasPerKeyState = false;
        static constexpr bool HasMigration   = false;
        static constexpr bool HasScanner     = false;
        static constexpr bool HasDesire      = false;
        static constexpr bool HasStoreEviction = true;
        static constexpr bool HasRemarcConfig   = false;

        static Config MakeConfig() noexcept { return {}; }
        static uint8_t InitialState() noexcept { return 0; }
    };

} // namespace NuAtlas
