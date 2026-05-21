#include "Numatic.h"
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#include <unistd.h>
#include <sys/mman.h>
#include <vector>
#include <fstream>
#include <string>

namespace NuAtlas::Numatic {

    bool IsNUMAAvailable() noexcept {
        return numa_available() != -1;
    }

    int GetNodeCount() noexcept {
        if (!IsNUMAAvailable()) return 1;
        return numa_max_node() + 1;
    }

    static thread_local int sNodeOverride = -1;

    int GetCurrentNode() noexcept {
        if (sNodeOverride >= 0) return sNodeOverride;
        thread_local int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    void SetCurrentNodeOverride(int nodeId) noexcept {
        sNodeOverride = nodeId;
    }

    void ClearCurrentNodeOverride() noexcept {
        sNodeOverride = -1;
    }

    void PinCurrentThreadToNode(int nodeId) noexcept {
        numa_run_on_node(nodeId);
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        struct bitmask* mask = numa_allocate_cpumask();
        numa_node_to_cpus(nodeId, mask);
        int maxCpu = numa_num_possible_cpus();
        for (int i = 0; i < maxCpu; i++) {
            if (numa_bitmask_isbitset(mask, i))
                CPU_SET(i, &cpuset);
        }
        numa_free_cpumask(mask);
        sched_setaffinity(0, sizeof(cpuset), &cpuset);
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

    bool IsHugePagesAvailable() noexcept {
        std::ifstream f("/proc/sys/vm/nr_hugepages");
        if (!f.is_open()) return false;
        long count = 0;
        f >> count;
        return count > 0;
    }

    size_t GetHugePageSize() noexcept {
        std::ifstream f("/proc/meminfo");
        if (!f.is_open()) return 0;
        std::string line;
        while (std::getline(f, line)) {
            if (line.find("Hugepagesize:") != std::string::npos) {
                size_t kb = 0;
                for (char c : line) {
                    if (c >= '0' && c <= '9') kb = kb * 10 + (c - '0');
                    else if (kb > 0) break;
                }
                return kb * 1024;
            }
        }
        return 0;
    }

    void* AllocateOnNodeHuge(size_t size, int nodeId) noexcept {
        void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr == MAP_FAILED) return nullptr;
        if (numa_available() != -1) {
            numa_set_preferred(nodeId);
            madvise(ptr, size, MADV_DONTFORK);
        }
        return ptr;
    }

    void* AllocateLocalHuge(size_t size) noexcept {
        void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr == MAP_FAILED) return nullptr;
        if (numa_available() != -1) {
            int node = GetCurrentNode();
            numa_set_preferred(node);
            madvise(ptr, size, MADV_DONTFORK);
        }
        return ptr;
    }

}
