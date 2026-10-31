#include "CMap.h"
#include "TestValue.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

using namespace NuAtlas;

namespace {

unsigned Workers() {
    unsigned hw = std::thread::hardware_concurrency();
    return hw > 2 ? hw - 1 : 2;
}

template <class Fn>
void RunFor(std::chrono::milliseconds duration, unsigned threads, Fn&& body) {
    std::atomic<bool> stop{false};
    std::vector<std::thread> pool;
    for (unsigned t = 0; t < threads; t++)
        pool.emplace_back([&, t] { body(t, stop); });
    std::this_thread::sleep_for(duration);
    stop.store(true);
    for (auto& th : pool) th.join();
}

}  // namespace

template <class Store>
class ConcurrentPolicyTest : public ::testing::Test {};

using Stores = ::testing::Types<ConcurrentLRU<TestValue>, ConcurrentARC<TestValue>,
                                ConcurrentS3FIFO<TestValue>>;
TYPED_TEST_SUITE(ConcurrentPolicyTest, Stores);

// Readers race erase/reinsert churn; a slot reused by another key between
// probe and read must never surface as this key's value.
TYPED_TEST(ConcurrentPolicyTest, ReadersNeverSeeAnotherKeysValue) {
    const size_t cap = 512, keyRange = 4 * cap;
    TypeParam store(cap);
    std::atomic<size_t> wrong{0}, hits{0};

    RunFor(std::chrono::milliseconds(750), Workers(), [&](unsigned t, std::atomic<bool>& stop) {
        std::mt19937_64 rng(t);
        while (!stop.load(std::memory_order_relaxed)) {
            uint64_t k = rng() % keyRange;
            switch (rng() % 4) {
            case 0: store.Set(Key(k), MakeValue(k)); break;
            case 1: store.Erase(Key(k)); break;
            default:
                if (auto v = store.Find(Key(k))) {
                    hits.fetch_add(1, std::memory_order_relaxed);
                    if (v->id != k) wrong.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    });
    EXPECT_GT(hits.load(), 0u);
    EXPECT_EQ(wrong.load(), 0u);
}

// Concurrent inserts of distinct keys, plus readers and in-place updaters that
// make evictions hit busy slots: every key must end up resident or reported
// through the eviction callback.
TYPED_TEST(ConcurrentPolicyTest, EveryKeyIsLiveOrEvicted) {
    const size_t cap = 1024;
    TypeParam store(cap);
    std::atomic<size_t> evicted{0};
    store.SetEvictionCallback([&](const TestValue&) { evicted.fetch_add(1, std::memory_order_relaxed); });

    const unsigned threads = Workers();
    std::vector<size_t> inserted(threads, 0);
    auto key = [](unsigned t, size_t i) { return "t" + std::to_string(t) + "-" + std::to_string(i); };

    RunFor(std::chrono::milliseconds(750), threads, [&](unsigned t, std::atomic<bool>& stop) {
        std::mt19937_64 rng(t);
        size_t& n = inserted[t];
        while (!stop.load(std::memory_order_relaxed)) {
            switch (rng() % 4) {
            case 0:
                if (store.Set(key(t, n), MakeValue(n)) == NO_ERR) n++;
                break;
            case 1:
                if (n) store.UpdateInPlace(key(t, rng() % n), [](TestValue& v) { v.DataSize++; });
                break;
            default:
                if (n) store.Find(key(t, n - 1 - rng() % std::min<size_t>(n, cap)));
            }
        }
    });

    size_t total = 0, live = 0;
    for (unsigned t = 0; t < threads; t++) {
        total += inserted[t];
        for (size_t i = 0; i < inserted[t]; i++) live += store.Find(key(t, i)).has_value();
    }
    EXPECT_GT(total, cap);
    EXPECT_LE(live, cap + 1);
    EXPECT_EQ(live + evicted.load(), total);
}
