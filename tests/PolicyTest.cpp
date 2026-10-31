#include "CMap.h"
#include "TestValue.h"
#include <gtest/gtest.h>
#include <random>

using namespace NuAtlas;

template <class Store>
class PolicyTest : public ::testing::Test {
protected:
    static constexpr size_t kCap = 1024;
    Store store{kCap};
    size_t evicted = 0;

    void SetUp() override {
        store.SetEvictionCallback([this](const TestValue&) { evicted++; });
    }

    size_t CountLive(size_t keyRange) {
        size_t live = 0;
        for (size_t i = 0; i < keyRange; i++) live += store.Find(Key(i)).has_value();
        return live;
    }
};

using Stores = ::testing::Types<ConcurrentLRU<TestValue>, ConcurrentARC<TestValue>,
                                ConcurrentS3FIFO<TestValue>>;
TYPED_TEST_SUITE(PolicyTest, Stores);

TYPED_TEST(PolicyTest, SetThenFind) {
    ASSERT_EQ(this->store.Set("a", MakeValue(1)), NO_ERR);
    auto found = this->store.Find("a");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->id, 1u);
    EXPECT_FALSE(this->store.Find("b").has_value());
}

TYPED_TEST(PolicyTest, UpdateDoesNotEvict) {
    for (int i = 0; i < 10; i++) ASSERT_EQ(this->store.Set("a", MakeValue(i)), NO_ERR);
    EXPECT_EQ(this->store.Find("a")->id, 9u);
    EXPECT_EQ(this->evicted, 0u);
}

TYPED_TEST(PolicyTest, EraseRemovesKey) {
    this->store.Set("a", MakeValue(1));
    auto erased = this->store.Erase("a");
    EXPECT_EQ(erased.err, NO_ERR);
    EXPECT_FALSE(this->store.Find("a").has_value());
    EXPECT_EQ(this->evicted, 0u);
}

// A stream of unique keys must fill the cache to capacity, and never more.
TYPED_TEST(PolicyTest, FillsToCapacity) {
    const size_t cap = TestFixture::kCap, n = 10 * cap;
    for (size_t i = 0; i < n; i++) ASSERT_EQ(this->store.Set(Key(i), MakeValue(i)), NO_ERR);
    size_t live = this->CountLive(n);
    EXPECT_LE(live, cap);
    EXPECT_GE(live, cap * 95 / 100);
}

// Every key ever inserted is either still resident or was reported through
// the eviction callback; anything else leaks the caller's page memory.
TYPED_TEST(PolicyTest, EveryKeyIsLiveOrEvicted) {
    const size_t cap = TestFixture::kCap, n = 20 * cap;
    std::mt19937_64 rng(42);
    for (size_t i = 0; i < n; i++) {
        ASSERT_EQ(this->store.Set(Key(i), MakeValue(i)), NO_ERR);
        // Re-read a few recent keys so frequency/recency policies move items around.
        for (int r = 0; r < 3; r++) this->store.Find(Key(i - rng() % std::min<size_t>(i + 1, cap)));
    }
    size_t live = this->CountLive(n);
    EXPECT_EQ(live + this->evicted, n);
    EXPECT_LE(live, cap);
}

TYPED_TEST(PolicyTest, ValuesMatchKeys) {
    const size_t n = 4 * TestFixture::kCap;
    for (size_t i = 0; i < n; i++) this->store.Set(Key(i), MakeValue(i));
    for (size_t i = 0; i < n; i++) {
        auto v = this->store.Find(Key(i));
        if (v) ASSERT_EQ(v->id, i);
    }
}

TYPED_TEST(PolicyTest, ForceEvictOne) {
    for (size_t i = 0; i < 10; i++) this->store.Set(Key(i), MakeValue(i));
    EXPECT_TRUE(this->store.ForceEvictOne());
    EXPECT_EQ(this->evicted, 1u);
    EXPECT_EQ(this->CountLive(10), 9u);
}

TYPED_TEST(PolicyTest, ForceEvictOnEmptyStoreFails) {
    EXPECT_FALSE(this->store.ForceEvictOne());
}

// --- Policy-specific behaviour ---

TEST(LRU, RecentlyReadKeySurvives) {
    ConcurrentLRU<TestValue> lru(8);
    for (size_t i = 0; i < 8; i++) lru.Set(Key(i), MakeValue(i));
    lru.Find(Key(0));
    lru.drainPromotes();
    lru.Set(Key(8), MakeValue(8));
    EXPECT_TRUE(lru.Find(Key(0)).has_value());
    EXPECT_FALSE(lru.Find(Key(1)).has_value());
}

// A key read twice while in Small must move to Main, not out of the cache.
TEST(S3FIFO, ReaccessedKeyGraduatesToMain) {
    ConcurrentS3FIFO<TestValue> s(1000);
    s.Set("hot", MakeValue(1));
    s.Find("hot");
    s.Find("hot");
    for (size_t i = 0; i < 200; i++) s.Set(Key(i), MakeValue(i));
    EXPECT_TRUE(s.Find("hot").has_value());
}

// One-hit wonders pass through Small without displacing Main.
TEST(S3FIFO, ScanResistance) {
    ConcurrentS3FIFO<TestValue> s(100);
    for (int i = 0; i < 50; i++) {
        s.Set("hot" + std::to_string(i), MakeValue(i));
        s.Find("hot" + std::to_string(i));
        s.Find("hot" + std::to_string(i));
    }
    for (size_t i = 0; i < 10000; i++) s.Set(Key(i), MakeValue(i));
    for (int i = 0; i < 50; i++) EXPECT_TRUE(s.Find("hot" + std::to_string(i)).has_value()) << i;
}

// A key evicted recently is re-admitted straight into Main, even after the
// ghost has seen far more evictions than it can hold.
TEST(S3FIFO, GhostReadmitsRecentlyEvictedKey) {
    ConcurrentS3FIFO<TestValue> s(100);
    for (size_t i = 0; i < 5000; i++) s.Set(Key(i), MakeValue(i));  // saturate the ghost
    s.Set("g", MakeValue(1));
    for (size_t i = 5000; i < 5150; i++) s.Set(Key(i), MakeValue(i));  // flush "g" out of the cache
    ASSERT_FALSE(s.Find("g").has_value());

    s.Set("g", MakeValue(2));  // ghost hit -> Main
    for (size_t i = 6000; i < 7000; i++) s.Set(Key(i), MakeValue(i));
    EXPECT_TRUE(s.Find("g").has_value());
}
