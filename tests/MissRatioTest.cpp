// Miss-ratio regression tests: replay synthetic traces through the concurrent
// policies and compare against exact reference implementations. Lost
// promotions, leaked entries or broken admission show up here as miss-ratio
// drift long before they show up as crashes.
#include "TraceSim.h"
#include <gtest/gtest.h>

using namespace TraceSim;

namespace {
    const Trace& ZipfTrace() {
        static const Trace t = Zipf(20000, 400000, 0.9, 7);
        return t;
    }
    const Trace& ScanTrace() {
        static const Trace t = ZipfWithScans(20000, 400000, 0.9, 0.3, 2000, 7);
        return t;
    }
    constexpr size_t kCaps[] = {200, 2000};
}

// With one thread, ConcurrentLRU must behave exactly like LRU: every deferred
// promotion has to be applied before the next eviction decision.
TEST(MissRatio, ConcurrentLRUMatchesExactLRU) {
    for (size_t cap : kCaps) {
        double ref = Replay("ref-lru", ZipfTrace(), cap).MissRatio();
        double lru = Replay("lru", ZipfTrace(), cap).MissRatio();
        EXPECT_NEAR(lru, ref, 0.002) << "cap=" << cap;
    }
}

TEST(MissRatio, NothingBeatsBelady) {
    for (size_t cap : kCaps)
        for (const auto& p : PolicyNames()) {
            ReplayResult r = Replay(p, ZipfTrace(), cap);
            EXPECT_EQ(r.failedSets, 0u) << p;
            EXPECT_GE(r.MissRatio() + 1e-9, Replay("opt", ZipfTrace(), cap).MissRatio()) << p << " cap=" << cap;
        }
}

// ARC and S3-FIFO both beat LRU on skewed traffic (Megiddo & Modha; Yang et
// al., SOSP'23); losing that edge means a policy bug.
TEST(MissRatio, AdaptivePoliciesBeatLRUOnZipf) {
    for (size_t cap : kCaps) {
        double lru = Replay("ref-lru", ZipfTrace(), cap).MissRatio();
        EXPECT_LT(Replay("arc", ZipfTrace(), cap).MissRatio(), lru - 0.02) << "cap=" << cap;
        EXPECT_LT(Replay("s3fifo", ZipfTrace(), cap).MissRatio(), lru - 0.02) << "cap=" << cap;
    }
}

TEST(MissRatio, ScanResistance) {
    for (size_t cap : kCaps) {
        double lru = Replay("ref-lru", ScanTrace(), cap).MissRatio();
        EXPECT_LT(Replay("arc", ScanTrace(), cap).MissRatio(), lru - 0.02) << "cap=" << cap;
        EXPECT_LT(Replay("s3fifo", ScanTrace(), cap).MissRatio(), lru - 0.02) << "cap=" << cap;
    }
}

// Concurrency must not change what the policy decides, beyond the reordering
// that interleaving introduces.
TEST(MissRatio, ThreadsDoNotDegradeMissRatio) {
    for (const char* p : {"lru", "arc", "s3fifo"}) {
        double one = Replay(p, ZipfTrace(), 2000, 1).MissRatio();
        double four = Replay(p, ZipfTrace(), 2000, 4).MissRatio();
        EXPECT_NEAR(four, one, 0.01) << p;
    }
}

TEST(TraceSim, BeladyOnTinyTrace) {
    // a b c a b d a b, cap 2: OPT keeps {a,b} after the compulsory misses.
    Trace t = {1, 2, 3, 1, 2, 4, 1, 2};
    EXPECT_EQ(BeladyMisses(t, 2), 4u);  // a, b, c (bypassed), d (bypassed)
    EXPECT_EQ(Replay("ref-lru", t, 2).misses, 8u);
}
