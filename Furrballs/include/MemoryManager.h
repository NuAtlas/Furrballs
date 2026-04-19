/*****************************************************************//**
 * \file   MemoryManager.h
 * \brief General-purpose memory allocation and management for Furrballs.
 *        NUMA-aware allocation is handled by Numatic.
 *
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#pragma once
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <unordered_set>
#include <mutex>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#endif
#ifdef _DEBUG
#define DEBUG
#endif

#include "Logger.h"

namespace NuAtlas {

    template<typename T>
    consteval size_t padded_size() {
        return (sizeof(T) + alignof(T) - 1) & ~(alignof(T) - 1);
    }

    constexpr size_t padded_size_to(size_t size, size_t alignment) {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    template<typename T, typename U>
    struct CoAllocPair {
        void* base = nullptr;
        T* first = nullptr;
        U* second = nullptr;
        size_t totalBytes = 0;
        size_t alignment  = 0;

        void dealloc() const {
            ::operator delete(base, std::align_val_t{alignment});
        }
    };

    template<typename T, typename U>
    CoAllocPair<T, U> CoAllocate(size_t countT, size_t countU) {
        constexpr size_t allocAlign = std::max(alignof(T), alignof(U));
        constexpr bool tFirst = alignof(T) >= alignof(U);

        size_t sizeT = countT * sizeof(T);
        size_t sizeU = countU * sizeof(U);
        size_t secondOffset = tFirst
            ? padded_size_to(sizeT, alignof(U))
            : padded_size_to(sizeU, alignof(T));
        size_t total = secondOffset + (tFirst ? sizeU : sizeT);

        void* base = ::operator new(total, std::align_val_t{allocAlign});

        T* tPtr;
        U* uPtr;
        if constexpr (tFirst) {
            tPtr = static_cast<T*>(base);
            uPtr = reinterpret_cast<U*>(static_cast<char*>(base) + secondOffset);
        } else {
            uPtr = static_cast<U*>(base);
            tPtr = static_cast<T*>(static_cast<char*>(base) + secondOffset);
        }

        return {base, tPtr, uPtr, total, allocAlign};
    }

    class MemoryManager {
    private:
        inline static std::mutex FreeingMutex;
        inline static std::mutex ProtectMutex;
        static thread_local std::unordered_set<void*> ThreadBuffers;

    public:
        [[nodiscard]] static void* AllocateMemory(size_t totalSize) {
#ifdef _WIN32
            void* buffer = VirtualAlloc(nullptr, totalSize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
            if (!buffer) {
                return nullptr;
            }
#else
            size_t pageSize = sysconf(_SC_PAGESIZE);
            void* buffer = nullptr;
            if (posix_memalign(&buffer, pageSize, totalSize) != 0) {
                return nullptr;
            }
#endif
#ifdef DEBUG
            Logger::getInstance().info("Thread " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + " Allocated " + std::to_string(totalSize));
#endif
            ThreadBuffers.insert(buffer);
            return buffer;
        }

        [[nodiscard]] static bool ProtectMemory(void* buffer, const size_t& size) {
            std::lock_guard<std::mutex> lock(ProtectMutex);
#ifdef _WIN32
            DWORD oldProtect;
            if (!VirtualProtect(buffer, size, PAGE_READWRITE, &oldProtect)) {
                Logger::getInstance().error("Failed to set memory protection on Windows");
                return false;
            }
#else
            if (mprotect(buffer, size, PROT_READ | PROT_WRITE) != 0) {
                Logger::getInstance().error("Failed to set memory protection on Linux");
                return false;
            }
#endif
            return true;
        }

        static void FreeMemory(void* buffer) {
            bool isOwner = IsThreadLocal(buffer);
            if (!isOwner) {
                std::lock_guard<std::mutex> lock(FreeingMutex);
            }
#ifdef _WIN32
            VirtualFree(buffer, 0, MEM_RELEASE);
#else
            free(buffer);
#endif
            ThreadBuffers.erase(buffer);
        }

        static size_t GetAvailableMemory() {
#ifdef _WIN32
            MEMORYSTATUSEX status;
            status.dwLength = sizeof(status);
            GlobalMemoryStatusEx(&status);
            return status.ullAvailPhys;
#else
            long pages = sysconf(_SC_AVPHYS_PAGES);
            long page_size = sysconf(_SC_PAGE_SIZE);
            return pages * page_size;
#endif
        }

        static size_t GetLargestContiguousBlock() {
            size_t step = 1024 * 1024;
            size_t maxBlockSize = 0;
            void* buffer = nullptr;

            while (true) {
                buffer = AllocateMemory(maxBlockSize + step);
                if (buffer) {
                    FreeMemory(buffer);
                    maxBlockSize += step;
                }
                else {
                    break;
                }
            }

            return maxBlockSize;
        }

        static inline bool IsThreadLocal(void* const& buffer) noexcept {
            return ThreadBuffers.find(buffer) != ThreadBuffers.cend();
        }
    };
}
