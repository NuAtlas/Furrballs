#pragma once
#include <cstddef>

namespace NuAtlas::Numatic {
    bool IsNUMAAvailable() noexcept;
    int GetNodeCount() noexcept;
    int GetCurrentNode() noexcept;
    void PinCurrentThreadToNode(int nodeId) noexcept;
    void SetPreferredNode(int nodeId) noexcept;
    size_t GetNodePageSize() noexcept;
    void* AllocateOnNode(size_t size, int nodeId) noexcept;
    void FreeNUMA(void* ptr, size_t size) noexcept;
    int GetAvailableNodes() noexcept;
}
