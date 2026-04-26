#include "Sandbox.h"
#include <iostream>
#include <cstring>
#include <vector>
#include <string>

using FB = NuAtlas::FurrBall<NuAtlas::StandardRemarc>;

int main() {
    FB::Bootstrap();

    NuAtlas::FurrConfig config;
    config.EnableLogging = true;
    config.InitialPageCount = 16;
    config.EnableNUMA = true;
    NuAtlas::NumaConfig numaConfig;
    config.numaConfig = &numaConfig;

    FB* fb = FB::CreateBall("TestDB", config);
    if (!fb) {
        std::cerr << "Error: Furrball has not initialized" << std::endl;
        return -1;
    }

    struct TestEntry {
        std::string key;
        std::string value;
    };

    std::vector<TestEntry> entries = {
        {"greeting", "Hello, NUMA-aware Furrballs!"},
        {"counter", "42"},
        {"config", "pagesize=4096"},
        {"status", "operational"},
        {"large", std::string(2000, 'X')},
    };

    char buf[4096];
    size_t outSize = 0;

    for (const auto& entry : entries) {
        NuAtlas::Error err = fb->Set(entry.key, (void*)entry.value.c_str(), entry.value.size());
        if (err != NuAtlas::NO_ERR) {
            std::cerr << "Set failed for key '" << entry.key << "': " << err << std::endl;
        }
    }

    for (const auto& entry : entries) {
        NuAtlas::Error err = fb->Get(entry.key, buf, sizeof(buf), outSize);
        if (err == NuAtlas::NO_ERR) {
            std::string readVal((char*)buf, outSize);
            bool match = (readVal == entry.value);
            std::cout << "[" << (match ? "OK" : "FAIL") << "] "
                      << entry.key << ": " << (match ? "matched" : "MISMATCH")
                      << " (size=" << outSize << ")" << std::endl;
        } else {
            std::cout << "[FAIL] " << entry.key << ": Get returned " << err << std::endl;
        }
    }

    NuAtlas::Error missErr = fb->Get("nonexistent", buf, sizeof(buf), outSize);
    std::cout << "["
              << (missErr == NuAtlas::INVALID_ARG ? "OK" : "FAIL")
              << "] nonexistent key: "
              << (missErr == NuAtlas::INVALID_ARG ? "correctly returned error" : "unexpected success")
              << std::endl;

    const char* updateData = "updated value!";
    NuAtlas::Error setErr = fb->Set("counter", (void*)updateData, std::strlen(updateData));
    if (setErr == NuAtlas::NO_ERR) {
        fb->Get("counter", buf, sizeof(buf), outSize);
        std::string readVal((char*)buf, outSize);
        bool match = (readVal == std::string(updateData));
        std::cout << "[" << (match ? "OK" : "FAIL") << "] overwrite: "
                  << (match ? "matched" : "MISMATCH") << std::endl;
    }

    std::cout << "\nStats:" << std::endl;
    std::cout << "  HitCount: " << fb->Stats.GetHitCount() << std::endl;
    std::cout << "  MissCount: " << fb->Stats.GetMissCount() << std::endl;
    std::cout << "  BytesWritten: " << fb->Stats.GetBytesWritten() << std::endl;
    std::cout << "  BytesRead: " << fb->Stats.GetBytesRead() << std::endl;
    std::cout << "  TotalAllocated: " << fb->Stats.GetTotalAllocated() << std::endl;

    delete fb;
    FB::Shutdown();
    return 0;
}
