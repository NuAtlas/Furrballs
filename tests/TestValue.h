#pragma once
#include <cstdint>
#include <string>

// Minimal stand-in for KeyMeta: the stores require DataOffset/DataSize.
struct TestValue {
    void* DataOffset = nullptr;
    uint32_t DataSize = 0;
    uint64_t id = 0;
};

inline TestValue MakeValue(uint64_t id) {
    return TestValue{reinterpret_cast<void*>(uintptr_t{1}), 8, id};
}

inline std::string Key(uint64_t id) { return "k" + std::to_string(id); }
