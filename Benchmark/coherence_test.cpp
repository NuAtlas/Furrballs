// coherence_test.cpp — Verify SET coherence: a key exists on exactly one node.
//
// On a non-NUMA system (VPS), this tests the logic path.
// On a NUMA system (EC2), this tests actual cross-node behavior.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>

#include "Furrballs.h"
#include "Policy.h"
#include "Numatic.h"

using namespace NuAtlas;

static bool test_passed = true;
static int tests_run = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (!(cond)) { \
        printf("FAIL: %s (line %d)\n", msg, __LINE__); \
        test_passed = false; \
    } else { \
        tests_passed++; \
        printf("PASS: %s\n", msg); \
    } \
} while(0)

int main() {
    auto& gs = Detail::globalNumaState;
    int nodeCount = 2;

    gs.NumaNodeCount = nodeCount;
    gs.Workers = (NodeJob*)malloc(sizeof(NodeJob) * nodeCount);
    for (int i = 0; i < nodeCount; i++) {
        new(&gs.Workers[i]) NodeJob(i);
        gs.Workers[i].Start([](){});
    }
    gs.Initialized = true;

    NumaConfig nc;
    nc.AllocateUsingNodePageSize = false;
    nc.UseThreadLocalRouting = true;

    FurrConfig fc;
    fc.PageSize = 4096;
    fc.TotalCapacityBytes = 4 * 1024 * 1024;
    fc.IsVolatile = true;
    fc.EnableNUMA = true;
    fc.numaConfig = &nc;

    auto* fb = FurrBall<ArcPolicy>::CreateBall("/tmp/coherence_test", fc, true);
    assert(fb != nullptr);

    uint8_t val1[] = {0xAA, 0xBB, 0xCC, 0xDD};
    uint8_t val2[] = {0x11, 0x22, 0x33, 0x44};
    uint8_t buf[64];
    size_t outSize = 0;

    // --- Test 1: Basic SET then GET ---
    printf("\n--- Test 1: Basic SET/GET ---\n");
    Error err = fb->Set("key1", val1, sizeof(val1));
    CHECK(err == NO_ERR, "SET key1");

    err = fb->Get("key1", buf, sizeof(buf), outSize);
    CHECK(err == NO_ERR, "GET key1 succeeds");
    CHECK(outSize == sizeof(val1), "GET key1 size correct");
    CHECK(memcmp(buf, val1, sizeof(val1)) == 0, "GET key1 value correct");

    // --- Test 2: SET same key from same "node" (both threads on node 0 in non-NUMA) ---
    printf("\n--- Test 2: Update existing key ---\n");
    err = fb->Set("key1", val2, sizeof(val2));
    CHECK(err == NO_ERR, "SET key1 update");

    err = fb->Get("key1", buf, sizeof(buf), outSize);
    CHECK(err == NO_ERR, "GET key1 after update succeeds");
    CHECK(memcmp(buf, val2, sizeof(val2)) == 0, "GET key1 after update value correct (val2)");

    // --- Test 3: SET from thread on node 0, SET from thread on node 1, GET from node 0 ---
    // Simulates the coherence scenario: key should be on the LAST writer's node.
    printf("\n--- Test 3: Cross-node SET coherence ---\n");

    // Thread 0 (node 0) sets key2
    std::thread t0([&]() {
        Numatic::PinCurrentThreadToNode(0);
        Error e = fb->Set("key2", val1, sizeof(val1));
        CHECK(e == NO_ERR, "Thread 0 SET key2 = val1");
    });
    t0.join();

    // Thread 1 (node 1) sets key2 to different value
    std::thread t1([&]() {
        Numatic::PinCurrentThreadToNode(1 % nodeCount);
        Error e = fb->Set("key2", val2, sizeof(val2));
        CHECK(e == NO_ERR, "Thread 1 SET key2 = val2");
    });
    t1.join();

    // Thread 0 (node 0) gets key2 — should find val2 (the latest write)
    std::thread t0b([&]() {
        Numatic::PinCurrentThreadToNode(0);
        size_t os = 0;
        Error e = fb->Get("key2", buf, sizeof(buf), os);
        CHECK(e == NO_ERR, "Thread 0 GET key2 succeeds");
        CHECK(os == sizeof(val2), "Thread 0 GET key2 size correct");
        bool match = (memcmp(buf, val2, sizeof(val2)) == 0);
        CHECK(match, "Thread 0 GET key2 = val2 (latest write)");
        if (!match) {
            printf("  Expected: ");
            for (size_t i = 0; i < sizeof(val2); i++) printf("%02X ", val2[i]);
            printf("\n  Got:      ");
            for (size_t i = 0; i < os; i++) printf("%02X ", buf[i]);
            printf("\n");
        }
    });
    t0b.join();

    // Thread 1 (node 1) gets key2 — should also find val2
    std::thread t1b([&]() {
        Numatic::PinCurrentThreadToNode(1 % nodeCount);
        size_t os = 0;
        Error e = fb->Get("key2", buf, sizeof(buf), os);
        CHECK(e == NO_ERR, "Thread 1 GET key2 succeeds");
        bool match = (memcmp(buf, val2, sizeof(val2)) == 0);
        CHECK(match, "Thread 1 GET key2 = val2 (latest write)");
    });
    t1b.join();

    // --- Test 4: No duplicate keys — SET creates key on one node only ---
    printf("\n--- Test 4: No duplicate values ---\n");
    {
        // Thread 0 creates key3
        std::thread ta([&]() {
            Numatic::PinCurrentThreadToNode(0);
            fb->Set("key3", val1, sizeof(val1));
        });
        ta.join();

        // Thread 1 overwrites key3
        std::thread tb([&]() {
            Numatic::PinCurrentThreadToNode(1 % nodeCount);
            fb->Set("key3", val2, sizeof(val2));
        });
        tb.join();

        // Both threads should see val2
        std::thread tc([&]() {
            Numatic::PinCurrentThreadToNode(0);
            size_t os = 0;
            Error e = fb->Get("key3", buf, sizeof(buf), os);
            CHECK(e == NO_ERR, "Thread 0 GET key3 succeeds");
            CHECK(memcmp(buf, val2, sizeof(val2)) == 0, "Thread 0 GET key3 = val2");
        });
        std::thread td([&]() {
            Numatic::PinCurrentThreadToNode(1 % nodeCount);
            size_t os = 0;
            Error e = fb->Get("key3", buf, sizeof(buf), os);
            CHECK(e == NO_ERR, "Thread 1 GET key3 succeeds");
            CHECK(memcmp(buf, val2, sizeof(val2)) == 0, "Thread 1 GET key3 = val2");
        });
        tc.join();
        td.join();
    }

    // --- Test 5: Alternating writes don't corrupt ---
    printf("\n--- Test 5: Alternating writes ---\n");
    {
        std::atomic<bool> stop{false};
        std::atomic<int> mismatches{0};

        std::thread w0([&]() {
            Numatic::PinCurrentThreadToNode(0);
            for (int i = 0; i < 500; i++) {
                uint8_t v = (uint8_t)(i & 0xFF);
                fb->Set("key_alt", &v, 1);
            }
        });

        std::thread w1([&]() {
            Numatic::PinCurrentThreadToNode(1 % nodeCount);
            for (int i = 0; i < 500; i++) {
                uint8_t v = (uint8_t)((i | 0x80) & 0xFF);
                fb->Set("key_alt", &v, 1);
            }
        });

        w0.join();
        w1.join();

        // Final read from both nodes should see the same value
        uint8_t buf0[4], buf1[4];
        size_t os0 = 0, os1 = 0;

        std::thread r0([&]() {
            Numatic::PinCurrentThreadToNode(0);
            fb->Get("key_alt", buf0, sizeof(buf0), os0);
        });
        std::thread r1([&]() {
            Numatic::PinCurrentThreadToNode(1 % nodeCount);
            fb->Get("key_alt", buf1, sizeof(buf1), os1);
        });
        r0.join();
        r1.join();

        CHECK(os0 == os1, "Both nodes see same size for key_alt");
        if (os0 > 0 && os1 > 0) {
            CHECK(buf0[0] == buf1[0], "Both nodes see same value for key_alt");
        }
    }

    // Cleanup
    delete fb;

    for (int i = 0; i < nodeCount; i++) {
        gs.Workers[i].Stop();
        gs.Workers[i].~NodeJob();
    }
    free(gs.Workers);
    gs = {};

    printf("\n=== Results: %d/%d tests passed ===\n", tests_passed, tests_run);
    return test_passed ? 0 : 1;
}
