#include "Numatic.h"
#include <windows.h>

namespace NuAtlas::Numatic {

bool IsNUMAAvailable() noexcept {
    ULONG highestNode;
    if (GetNumaHighestNodeNumber(&highestNode)) {
        return highestNode > 0;
    }
    return false;
}

int GetNodeCount() noexcept {
    ULONG highestNode;
    if (GetNumaHighestNodeNumber(&highestNode)) {
        return static_cast<int>(highestNode + 1);
    }
    return 1;
}

int GetCurrentNode() noexcept {
    USHORT node;
    if (GetNumaProcessorNode(GetCurrentProcessorNumber(), &node)) {
        return static_cast<int>(node);
    }
    return 0;
}

void PinCurrentThreadToNode(int nodeId) noexcept {
    UCHAR nodeMask = 1 << nodeId;
    SetThreadAffinityMask(GetCurrentThread(), reinterpret_cast<ULONG_PTR>(&nodeMask));
}

void SetPreferredNode(int nodeId) noexcept {
    // Windows prefers via VirtualAllocExNuma at allocation time
}

size_t GetNodePageSize() noexcept {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwPageSize;
}

void* AllocateOnNode(size_t size, int nodeId) noexcept {
    return VirtualAllocExNuma(GetCurrentProcess(), nullptr, size,
        MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE, nodeId);
}

void FreeNUMA(void* ptr, size_t size) noexcept {
    VirtualFree(ptr, 0, MEM_RELEASE);
}

int GetAvailableNodes() noexcept {
    return GetNodeCount();
}

}
