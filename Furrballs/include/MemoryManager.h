/*****************************************************************//**
 * \file   MemoryManager.h
 * \brief Memory allocation and management for Furrballs.
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
#include <type_traits>
#include <Logger.h>
#include <mutex>
#include <list>
#include <optional>
#include <cassert>
#include <iostream>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#endif
#ifdef _DEBUG
#define DEBUG
#endif

#ifndef NO_NUMA
#ifdef __linux__
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#endif
#endif

 //Furrball, compact and filled with spit !
namespace NuAtlas {
    class MemoryManager {
    private:
        inline static std::mutex FreeingMutex;
        inline static std::mutex ProtectMutex;

        static thread_local std::unordered_set<void*> ThreadBuffers;
#ifndef NO_NUMA
        static int GetCurrentNumaNode() {
#ifdef _WIN32
            ULONG highestNodeNumber;
            GetNumaHighestNodeNumber(&highestNodeNumber);
            return GetCurrentProcessorNumber() % (highestNodeNumber + 1);
#elif defined(__linux__)
            return numa_node_of_cpu(sched_getcpu());
#else
            return 0;
#endif
        }
#endif

    public:
#ifndef NO_NUMA
        static void* AllocateMemoryNUMA(size_t totalSize) {
#ifdef _WIN32
            int node = GetCurrentNumaNode();
            void* buffer = VirtualAllocExNuma(GetCurrentProcess(), nullptr, totalSize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE, node);
            if (!buffer) {
                return nullptr;
            }
#elif defined(__linux__)
            int node = GetCurrentNumaNode();
            void* buffer = numa_alloc_onnode(totalSize, node);
            if (!buffer) {
                return nullptr;
            }
#else
            void* buffer = AllocateMemory(totalSize);
#endif
#ifdef DEBUG
            Logger::getInstance().info("Thread " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + " Allocated (NUMA) " + std::to_string(totalSize));
#endif
            return buffer;
        }
#endif
        static void* AllocateMemory(size_t totalSize) {
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
        static bool ProtectMemory(void* buffer, const size_t& size) {
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

        static bool IsNUMASystem() {
#ifdef NO_NUMA
            return false;
#elif defined(_WIN32)
            ULONG highestNodeNumber;
            if (GetNumaHighestNodeNumber(&highestNodeNumber)) {
                return highestNodeNumber > 0;
            }
            return false;
#elif defined(__linux__)
            return numa_available() != -1;
#else
            return false;
#endif
        }
    };
}