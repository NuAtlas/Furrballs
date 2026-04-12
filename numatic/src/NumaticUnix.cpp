#include "Numatic.h"
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#include <unistd.h>

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

}
