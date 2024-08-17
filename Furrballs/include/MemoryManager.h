/*****************************************************************//**
 * \file   Furrballs.h
 * \brief Primary interface for the Furrball library.
 *
 * This file contains the main classes and functions that users will interact with when using the Furrball library.
 * The library provides a caching and database management system using RocksDB and various caching policies.
 *
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#pragma once
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <type_traits>
#include <Logger.h>
#include <mutex>
#include <list>
#include <optional>
#include <cassert>


#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif
#ifdef _DEBUG
#define DEBUG
#endif

 //Furrball, compact and filled with spit !
namespace NuAtlas {
    /**
     * @brief Main Memory Allocator/Manager. Now Adding NUMA-Awareness.
     *
     * MemoryManager uses 2 Mutexes each to protect Freeing Memory (Critical) and Memory Protection. (on ProtectMemory call).
     * Allocation on the other hand is not locking nor protected.
     */
    class MemoryManager {
    private:
        inline static thread_local std::mutex FreeingMutex;
        inline static std::mutex ProtectMutex;

        static thread_local std::unordered_set<void*> ThreadBuffers;

        static int GetCurrentNumaNode() {
#ifdef _WIN32
            ULONG highestNodeNumber;
            GetNumaHighestNodeNumber(&highestNodeNumber);
            return GetCurrentProcessorNumber() % (highestNodeNumber + 1);
#else
            return numa_node_of_cpu(sched_getcpu());
#endif
        }

    public:
#ifndef NO_NUMA
        /**
         * @brief NUMA-Aware allocator.
         */
        static void* AllocateMemoryNUMA(size_t totalSize) {
#ifdef _WIN32
            
            
            int node = GetCurrentNumaNode();
            void* buffer = VirtualAllocExNuma(GetCurrentProcess(), nullptr, totalSize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE, node);
            if (!buffer) {
                return nullptr;

            }
#else
            int node = GetCurrentNumaNode();
            void* buffer = numa_alloc_onnode(totalSize, node);
            if (!buffer) {
                return nullptr;
            }
#endif //OS
#ifdef DEBUG
            Logger::getInstance().info("Thread " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "Allocated (NUMA)" + std::to_string(totalSize));
#endif //DEBUG
            return buffer;
        }
#endif //NO_NUMA
        /**
         * @brief Allocates Memory using Platform Specific calls.
         */
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
#endif //_WIN32
#ifdef DEBUG
            Logger::getInstance().info("Thread " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "Allocated " + std::to_string(totalSize));
#endif //DEBUG
            ThreadBuffers.insert(buffer);
            return buffer;
        }
        /**
         * @brief Protects the provided memory buffer.
         */
        static bool ProtectMemory(void* buffer, const size_t& size) {
            if (IsThreadLocal(buffer)) {
                std::lock_guard<std::mutex> lock(ProtectMutex);
#ifdef DEBUG
                std::cout << ("Did you intend to Protect a non thread_local buffer ?") << std::endl;
                DebugBreak();
#endif //DEBUG
            }
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
#endif //_WIN32
            return true;
        }
        /**
         * @brief Frees allocated buffer.
         *
         * FreeMemory is a blocking operation for freeing memory not owned by the thread.
         */
        static void FreeMemory(void* buffer) {
            if (IsThreadLocal(buffer)) {
#ifdef DEBUG
                std::cout << ("Did you intend to Free a non thread_local buffer ?") << std::endl;
                DebugBreak();
#endif  //DEBUG
                std::lock_guard<std::mutex> lock(FreeingMutex);
#ifdef _WIN32
                VirtualFree(buffer, 0, MEM_RELEASE);
#else
                free(buffer);
#endif //_WIN32
                ThreadBuffers.erase(buffer);
                return;
            }
            //Lock-less since the buffer is Thread Local, Ensuring the buffer is not used elsewhere is not the responsability of the allocator.
#ifdef _WIN32
            VirtualFree(buffer, 0, MEM_RELEASE);
#else
            free(buffer);
#endif  //_WIN32
        }
        /**
         * @brief Returns the available memory, works for both Windows and Unix
         */
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
#endif  //_WIN32
        }
        /**
         * @brief Utility Function: Attempts to allocate increasingly larger blocks of memory until it fails, then returns the size of the largest successful allocation.
         */
        static size_t GetLargestContiguousBlock() {
            size_t step = 1024 * 1024; // Start with 1MB
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
        /**
         * @brief Returns true if the buffer is allocated by the calling thread.
         */
        static inline bool IsThreadLocal(void* const& buffer) noexcept {
            return ThreadBuffers.find(buffer) != ThreadBuffers.cend();
        }

        bool IsNUMASystem() {
#ifdef NO_NUMA
            return false;
#endif  //NO_NUMA
#ifdef WIN32
            ULONG highestNodeNumber;
            if (GetNumaHighestNodeNumber(&highestNodeNumber)) {
                return highestNodeNumber > 0;
            }
            return false;
#elif
            //libnuma dependency.
            return numa_available() != -1;
#endif  //_WIN32
        }
    };