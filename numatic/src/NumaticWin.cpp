#include "Numatic.h"

#ifdef _WIN32
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

    void* AllocateLocal(size_t size) noexcept {
        return VirtualAlloc(nullptr, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    }

    void FreeNUMA(void* ptr, size_t size) noexcept {
        VirtualFree(ptr, 0, MEM_RELEASE);
    }

    int GetAvailableNodes() noexcept {
        return GetNodeCount();
    }

    int GetDistance(int nodeA, int nodeB) noexcept {
        if (nodeA == nodeB) return 10;
        return 20;
    }

    bool MovePages(void** pages, int count, int targetNode, int* status) noexcept {
        for (int i = 0; i < count; i++) {
            size_t pageSize;
            MEMORY_BASIC_INFORMATION mbi;
            if (VirtualQuery(pages[i], &mbi, sizeof(mbi)) == 0) {
                status[i] = -1;
                continue;
            }
            pageSize = mbi.RegionSize;
            void* newPage = AllocateOnNode(pageSize, targetNode);
            if (!newPage) {
                status[i] = -1;
                continue;
            }
            memcpy(newPage, pages[i], pageSize);
            VirtualFree(pages[i], 0, MEM_RELEASE);
            pages[i] = newPage;
            status[i] = targetNode;
        }
        return true;
    }

    bool MigratePages(int pid, const unsigned long* oldNodeMask, const unsigned long* newNodeMask) noexcept {
        return false;
    }

    int GetPageNode(void* page) noexcept {
        if (!page) return -1;
        MEMORY_BASIC_INFORMATION mbi;
        if (VirtualQuery(page, &mbi, sizeof(mbi)) == 0) return -1;
        return static_cast<int>(mbi.Node);
    }

    bool IsHugePagesAvailable() noexcept {
        return false;
    }

    size_t GetHugePageSize() noexcept {
        return 0;
    }

    void* AllocateOnNodeHuge(size_t, int) noexcept {
        return nullptr;
    }

    void* AllocateLocalHuge(size_t) noexcept {
        return nullptr;
    }

}

#endif
