#include "Numatic.h"
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#include <unistd.h>
#include <sys/mman.h>
#include <vector>

namespace NuAtlas::Numatic {

    bool IsNUMAAvailable() noexcept {
        return numa_available() != -1;
    }

    int GetNodeCount() noexcept {
        if (!IsNUMAAvailable()) return 1;
        return numa_max_node() + 1;
    }

    int GetCurrentNode() noexcept {
        return numa_node_of_cpu(sched_getcpu());
    }

    void PinCurrentThreadToNode(int nodeId) noexcept {
        numa_run_on_node(nodeId);
    }

    void SetPreferredNode(int nodeId) noexcept {
        numa_set_preferred(nodeId);
    }

    size_t GetNodePageSize() noexcept {
        return static_cast<size_t>(numa_pagesize());
    }

    void* AllocateOnNode(size_t size, int nodeId) noexcept {
        return numa_alloc_onnode(size, nodeId);
    }

    void* AllocateLocal(size_t size) noexcept {
        return numa_alloc_local(size);
    }

    void FreeNUMA(void* ptr, size_t size) noexcept {
        numa_free(ptr, size);
    }

    int GetAvailableNodes() noexcept {
        if (!IsNUMAAvailable()) return 0;
        bitmask* mask = numa_get_mems_allowed();
        int count = 0;
        for (int i = 0; i <= numa_max_node(); i++) {
            if (numa_bitmask_isbitset(mask, i)) count++;
        }
        numa_bitmask_free(mask);
        return count;
    }

    int GetDistance(int nodeA, int nodeB) noexcept {
        if (!IsNUMAAvailable()) return 0;
        return numa_distance(nodeA, nodeB);
    }

    bool MovePages(void** pages, int count, int targetNode, int* status) noexcept {
        if (!pages || count <= 0 || !status) return false;
        std::vector<int> nodes(count, targetNode);
        long ret = numa_move_pages(0, count, pages, nodes.data(), status, 0);
        return ret == 0;
    }

    bool MigratePages(int pid, const unsigned long* oldNodeMask, const unsigned long* newNodeMask) noexcept {
        if (!oldNodeMask || !newNodeMask) return false;
        int maxnode = GetNodeCount();
        bitmask* from = numa_bitmask_alloc(maxnode);
        bitmask* to = numa_bitmask_alloc(maxnode);
        for (int i = 0; i < maxnode; i++) {
            if (oldNodeMask[i / (sizeof(unsigned long) * 8)] & (1UL << (i % (sizeof(unsigned long) * 8)))) {
                numa_bitmask_setbit(from, i);
            }
            if (newNodeMask[i / (sizeof(unsigned long) * 8)] & (1UL << (i % (sizeof(unsigned long) * 8)))) {
                numa_bitmask_setbit(to, i);
            }
        }
        long ret = numa_migrate_pages(pid, from, to);
        numa_bitmask_free(from);
        numa_bitmask_free(to);
        return ret == 0;
    }

    int GetPageNode(void* page) noexcept {
        if (!page) return -1;
        int status = -1;
        void* addr = page;
        long ret = numa_move_pages(0, 1, &addr, nullptr, &status, 0);
        if (ret != 0) return -1;
        return status;
    }

}
