#pragma once
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <new>

namespace NuAtlas::Numatic {
    bool IsNUMAAvailable() noexcept;
    int GetNodeCount() noexcept;
    int GetCurrentNode() noexcept;
    void PinCurrentThreadToNode(int nodeId) noexcept;
    void SetPreferredNode(int nodeId) noexcept;
    size_t GetNodePageSize() noexcept;
    [[nodiscard]] void* AllocateOnNode(size_t size, int nodeId) noexcept;
    [[nodiscard]] void* AllocateLocal(size_t size) noexcept;
    void FreeNUMA(void* ptr, size_t size) noexcept;
    int GetAvailableNodes() noexcept;

    int GetDistance(int nodeA, int nodeB) noexcept;

    bool MovePages(void** pages, int count, int targetNode, int* status) noexcept;
    bool MigratePages(int pid, const unsigned long* oldNodeMask, const unsigned long* newNodeMask) noexcept;
    int GetPageNode(void* page) noexcept;

    template<typename Derived>
    class NumaMemoryResourceBase : public std::pmr::memory_resource {
    protected:
        static constexpr std::size_t PTR_SIZE = sizeof(void*);
        static constexpr std::size_t MAX_ALIGN = alignof(std::max_align_t);

        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            if (alignment <= MAX_ALIGN) {
                void* ptr = derived().alloc_raw(bytes);
                if (!ptr) throw std::bad_alloc();
                return ptr;
            }
            return allocate_aligned(bytes, alignment);
        }

        void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) noexcept override {
            if (!ptr) return;
            if (alignment <= MAX_ALIGN) {
                derived().free_raw(ptr, bytes);
            } else {
                void* raw = *reinterpret_cast<void**>(reinterpret_cast<std::uintptr_t>(ptr) - PTR_SIZE);
                derived().free_raw(raw, PTR_SIZE + alignment - 1 + bytes);
            }
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
            return this == &other;
        }

    private:
        void* allocate_aligned(std::size_t bytes, std::size_t alignment) {
            std::size_t total = PTR_SIZE + alignment - 1 + bytes;
            void* raw = derived().alloc_raw(total);
            if (!raw) throw std::bad_alloc();

            std::uintptr_t base = reinterpret_cast<std::uintptr_t>(raw) + PTR_SIZE;
            std::uintptr_t aligned = (base + alignment - 1) & ~(alignment - 1);
            *reinterpret_cast<void**>(aligned - PTR_SIZE) = raw;
            return reinterpret_cast<void*>(aligned);
        }

        Derived& derived() noexcept { return static_cast<Derived&>(*this); }
        const Derived& derived() const noexcept { return static_cast<const Derived&>(*this); }
    };

    class NumaLocalMemoryResource final : public NumaMemoryResourceBase<NumaLocalMemoryResource> {
    public:
        void* alloc_raw(std::size_t bytes) noexcept { return AllocateLocal(bytes); }
        void free_raw(void* ptr, std::size_t bytes) noexcept { FreeNUMA(ptr, bytes); }
    };

    class NumaNodeMemoryResource final : public NumaMemoryResourceBase<NumaNodeMemoryResource> {
    private:
        int node_;
    public:
        explicit NumaNodeMemoryResource(int nodeId) noexcept : node_(nodeId) {}

        void* alloc_raw(std::size_t bytes) noexcept { return AllocateOnNode(bytes, node_); }
        void free_raw(void* ptr, std::size_t bytes) noexcept { FreeNUMA(ptr, bytes); }
    };
}
