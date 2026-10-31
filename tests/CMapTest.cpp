#include "CMap.h"
#include "TestValue.h"
#include <gtest/gtest.h>

using namespace NuAtlas;

TEST(CMap, SetReportsInsertThenUpdate) {
    CMap<TestValue> m(64);
    auto first = m.Set("a", MakeValue(1));
    EXPECT_EQ(first.err, NO_ERR);
    EXPECT_TRUE(first.inserted);

    auto second = m.Set("a", MakeValue(2));
    EXPECT_EQ(second.err, NO_ERR);
    EXPECT_FALSE(second.inserted);

    auto found = m.Find("a");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->id, 2u);
}

TEST(CMap, MissingKeyIsNotFound) {
    CMap<TestValue> m(64);
    m.Set("a", MakeValue(1));
    EXPECT_FALSE(m.Find("b").has_value());
    EXPECT_EQ(m.Erase("b"), KEY_NOT_FOUND);
}

TEST(CMap, HoldsRequestedCapacity) {
    for (size_t cap : {1u, 15u, 16u, 1000u, 1024u, 4096u}) {
        CMap<TestValue> m(cap);
        for (size_t i = 0; i < cap; i++)
            ASSERT_EQ(m.Set(Key(i), MakeValue(i)).err, NO_ERR) << "cap=" << cap << " i=" << i;
        for (size_t i = 0; i < cap; i++)
            ASSERT_TRUE(m.Find(Key(i)).has_value()) << "cap=" << cap << " i=" << i;
    }
}

TEST(CMap, EraseReturnsValueAndRemovesKey) {
    CMap<TestValue> m(64);
    m.Set("a", MakeValue(7));
    auto erased = m.FindAndErase("a");
    EXPECT_EQ(erased.err, NO_ERR);
    ASSERT_TRUE(erased.value.has_value());
    EXPECT_EQ(erased.value->id, 7u);
    EXPECT_FALSE(m.Find("a").has_value());
}

TEST(CMap, ReinsertedKeyHasZeroedAux) {
    CMap<TestValue> m(64);
    HashPair h = HashKey("a");
    m.Set("a", MakeValue(1));
    auto ref = m.FindWithAux(h);
    ASSERT_TRUE(ref.has_value());
    std::atomic_ref<uint8_t>(*const_cast<uint8_t*>(&ref->aux[0])).store(5);

    m.Erase("a");
    m.Set("a", MakeValue(2));
    auto again = m.FindWithAux(h);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->aux[0], 0);
}

TEST(CMap, UpdateInPlace) {
    CMap<TestValue> m(64);
    m.Set("a", MakeValue(1));
    EXPECT_EQ(m.UpdateInPlace("a", [](TestValue& v) { v.id = 99; }), NO_ERR);
    EXPECT_EQ(m.Find("a")->id, 99u);
    EXPECT_EQ(m.UpdateInPlace("missing", [](TestValue&) {}), KEY_NOT_FOUND);
}

// Erase/insert churn at the maximum load factor must not fill the table with
// tombstones; once every free slot is a tombstone, each miss scans the table.
TEST(CMap, TombstonesStayBoundedUnderChurn) {
    const size_t slots = 4096;
    const size_t live = slots * 7 / 8;  // the most CMap(live) will ever hold
    CMap<TestValue> m(live);
    ASSERT_EQ(m.SlotCount(), slots);

    for (size_t i = 0; i < live; i++) ASSERT_EQ(m.Set(Key(i), MakeValue(i)).err, NO_ERR);
    for (size_t next = live; next < live + 50 * slots; next++) {
        ASSERT_EQ(m.Erase(Key(next - live)), NO_ERR);
        ASSERT_EQ(m.Set(Key(next), MakeValue(next)).err, NO_ERR);
    }

    // Without purging, all 512 free slots end up as tombstones.
    EXPECT_LT(m.TombstoneCount(), slots / 16);

    // Purges move entries: every live key must still map to its own value.
    size_t first = live + 50 * slots - live;
    for (size_t i = first; i < first + live; i++) {
        auto v = m.Find(Key(i));
        ASSERT_TRUE(v.has_value()) << i;
        ASSERT_EQ(v->id, i);
    }
    EXPECT_FALSE(m.Find(Key(first - 1)).has_value());
}

TEST(PromoteBuf, DrainsInOrderAndRecoversAfterOverflow) {
    PromoteBuf buf;
    size_t accepted = 0;
    for (uint64_t i = 0; i < 5000; i++) accepted += buf.enqueue(HashPair{i, i});
    EXPECT_EQ(accepted, 4096u);

    uint64_t expect = 0;
    EXPECT_EQ(buf.drainWith([&](HashPair h) { EXPECT_EQ(h.h1, expect++); }, 10), 10u);

    // Freed slots are reusable and the consumer is not stuck behind a hole.
    for (uint64_t i = 0; i < 10; i++) EXPECT_TRUE(buf.enqueue(HashPair{100000 + i, 0}));
    size_t drained = buf.drainWith([&](HashPair h) {
        if (expect < 4096) EXPECT_EQ(h.h1, expect++);
    });
    EXPECT_EQ(drained, 4096u);
    EXPECT_EQ(buf.pending(), 0u);
}
