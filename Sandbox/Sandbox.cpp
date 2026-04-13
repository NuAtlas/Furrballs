#include "Sandbox.h"
#include <iostream>
#include <cstring>

int main() {
    FurrBall::Bootstrap();

    FurrConfig config;
    config.EnableLogging = true;
    config.InitialPageCount = 16;
    config.EnableNUMA = true;
    NumaConfig numaConfig;
    config.numaConfig = &numaConfig;

    FurrBall* fb = FurrBall::CreateBall("TestDB", config);
    if (!fb) {
        std::cerr << "Error: Furrball has not initialized" << std::endl;
        return -1;
    }

    const char* testData = "Hello, NUMA-aware Furrballs!";
    size_t testAddr = 4096;

    if (!fb->Set((void*)testData, std::strlen(testData) + 1, testAddr)) {
        std::cerr << "Set failed" << std::endl;
        delete fb;
        return -1;
    }

    void* result = fb->Get((void*)testAddr);
    if (result) {
        std::cout << "Read back: " << static_cast<char*>(result) << std::endl;
    }
    else {
        std::cerr << "Get failed" << std::endl;
    }

    std::cout << "Stats:" << std::endl;
    std::cout << "  HitCount: " << fb->Stats.GetHitCount() << std::endl;
    std::cout << "  MissCount: " << fb->Stats.GetMissCount() << std::endl;
    std::cout << "  EvictionCount: " << fb->Stats.GetEvictionCount() << std::endl;
    std::cout << "  TotalAllocated: " << fb->Stats.GetTotalAllocated() << std::endl;

    delete fb;
    FurrBall::Shutdown();
    return 0;
}
