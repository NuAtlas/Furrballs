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
#include <filesystem>
#include <thread>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/advanced_cache.h>
#include <unordered_map>
#include <type_traits>
#include <Logger.h>
#include <mutex>
#include <optional>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

//Furrball, compact and filled with spit !
namespace Furrball {
    class MemoryManager {
    private:
        inline static std::mutex FreeingMutex;
        inline static std::mutex ProtectMutex;
    public:
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
            return buffer;
        }
        /**
         * .
         */
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
        /**
         * @brief Frees allocated buffer.
         * 
         * FreeMemory is a blocking operation.
         */
        static void FreeMemory(void* buffer) {
            std::lock_guard<std::mutex> lock(FreeingMutex);
#ifdef _WIN32
            VirtualFree(buffer, 0, MEM_RELEASE);
#else
            free(buffer);
#endif
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
#endif
        }
        /**
         * @brief Attempts to allocate increasingly larger blocks of memory until it fails, then returns the size of the largest successful allocation.
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
    };

    /**
     * @brief Implements the ARC eviction policy
     * TODO: Implement Adaptive Memory Pooling (AMP)
     * You can create and manage your own cache separately by instantiating a Policy object and using it.
     * @see S3FIFOPolicy
     * @see LRUPolicy
     * @see LFUPolicy
     */
    template<class Key, class Value>
    class ARCPolicy {
    public:
        typedef void(*EvictionCallback)(Key&);
    private:
        std::list<Key> t1;  // Recently added
        std::list<Key> t2;  // Recently used
        std::list<Key> b1;  // Ghost entries for t1
        std::list<Key> b2;  // Ghost entries for t2
        std::unordered_map<Key, Value> map;  // Key to value mapping
        std::list<Key> Window; // t1 + t2 (the cache)
        std::list<Key> GhostEntries; // b1 + b2 Ghost entries
        size_t capacity;
        size_t p;  // Target size for t1
        EvictionCallback evictionCallback = []() {};//NO-OP by default.

        void replace(const Key& key) {
            if (!t1.empty() && (t1.size() > p || (std::find(b2.begin(), b2.end(), key) != b2.end() && t1.size() == p))) {
                // Move from t1 to b1
                auto old = t1.back();
                t1.pop_back();
                b1.push_front(old);
                map.erase(old);
            }
            else {
                // Move from t2 to b2
                auto old = t2.back();
                t2.pop_back();
                b2.push_front(old);
                map.erase(old);
            }
        }

        void evict() {
            evictionCallback();
            if (t1.size() + b1.size() >= capacity) {
                if (t1.size() < capacity) {
                    evictionCallback(b1.back())
                    b1.pop_back();
                }
                else {
                    evictionCallback(t1.back())
                    t1.pop_back();
                }
            }
            if (t1.size() + t2.size() + b1.size() + b2.size() >= 2 * capacity) {
                if (t2.size() + b2.size() > capacity) {
                    evictionCallback(b2.back())
                    b2.pop_back();
                }
                else {
                    evictionCallback(t2.back())
                    t2.pop_back();
                }
            }
        }

    public:
        /**
         * @brief Creates a cache following ARC policy.
         * @param cap Capacity of the cache.
         */
        ARCPolicy(size_t cap) : capacity(cap), p(1) {}

        void setEvictionCallback(EvictionCallback cb) {
            evictionCallback = cb;
        };
        /**
         * @return true if the key exists.
         */
        bool contains(const Key& key) const {
            return map.find(key) != map.end();
        }
        /**
         * @brief Promotes a Key.
         */
        void touch(const Key& key) {
            if (std::find(t1.begin(), t1.end(), key) != t1.end()) {
                t1.remove(key);
                t2.push_front(key);
            }
            else if (std::find(t2.begin(), t2.end(), key) != t2.end()) {
                t2.splice(t2.begin(), t2, std::find(t2.begin(), t2.end(), key));
            }
            else if (std::find(b1.begin(), b1.end(), key) != b1.end()) {
                // Case when the key is in b1
                p = std::min(capacity, p + std::max(b2.size() / b1.size(), 1UL));
                replace(key);
                b1.remove(key);
                t2.push_front(key);
                map[key] = Value();  // Assuming default constructor exists
            }
            else if (std::find(b2.begin(), b2.end(), key) != b2.end()) {
                // Case when the key is in b2
                p = std::max(0, static_cast<int>(p) - std::max(b1.size() / b2.size(), 1UL));
                replace(key);
                b2.remove(key);
                t2.push_front(key);
                map[key] = Value();  // Assuming default constructor exists
            }
        }
        /**
         * @brief Adds a Key-Value Pair the the cache.
         */
        void add(const Key& key, const Value& value) {
            if (map.size() >= capacity) {
                evict();
            }
            t1.push_front(key);
            map[key] = value;
        }
        /**
         * @brief Gets a value from the cache.
         */
        Value get(const Key& key) {
            touch(key);
            return map[key];
        }
        /**
         * @brief Changes a value if it exsits or adds it.
         */
        void set(const Key& key, const Value& value) {
            if (contains(key)) {
                map[key] = value;
                touch(key);
            }
            else {
                add(key, value);
            }
        }
    };
    /**
     * @brief Implements the S3FIFO eviction policy
     *
     * You can Create and manage your own cache seperatly by instanciating a Policy Object and use it.
     * @see ARCPolicy
     * @see LRUPolicy
     * @see LFUPolicy
     */
    template<class Key, class Value>
    class S3FIFOPolicy final {
    private:
        std::list<Key> queue;
        std::unordered_map<Key, Value> map;
        size_t capacity;
    public:

    };
    /**
     * @brief Implements the LRU eviction policy
     *
     * You can Create and manage your own cache seperatly by instanciating a Policy Object and use it.
     * @see ARCPolicy
     * @see S3FIFOPolicy
     * @see LFUPolicy
     */
    template<class Key, class Value>
    class LRUPolicy final {

    };
    /**
     * @brief Implements the LFU eviction policy
     *
     * You can Create and manage your own cache seperatly by instanciating a Policy Object and use it.
     * @see ARCPolicy
     * @see LRUPolicy
     * @see S3FIFOPolicy
     */
    template<class Key, class Value>
    class LFUPolicy final {

    };

    /**
    * @brief A Cache with size_t as Keys, void* as values and ARC eviction policy.
    */
    //typedef Cache<size_t, void*, ARCPolicy> ARCCache;
    /**
    * @class FurrBall
    * @brief Furrballs are a LZ4 Compressed DB using RocksDB with Cache and Paging Logic.
    */
    class FurrBall final {
    private:
        struct ImplDetail;
        std::unique_ptr<ImplDetail> DataMembers;

        size_t PageSize;
        //Must remain POD.
        /**
         * \brief Page Metadata.
         */
        struct Page {
            void* ptr;
            size_t PageIndex;

            Page() {

            }

            virtual bool IsLockable()const noexcept { return false; };

            ~Page() {

            }
        };
        struct LockablePage : public Page {
            virtual bool IsLockable()const noexcept { return true; };
        };
        std::list<Page> PageList;
        /**
         * @brief Cache Object.
         */
        //ARCPolicy<size_t,void*> ARC = ARCPolicy<size_t,void*>();

        FurrBall()noexcept = default;
        FurrBall(std::unique_ptr<ImplDetail>);

        constexpr size_t floorAddress(size_t address)const noexcept {
            return address & ~(PageSize - 1);
        }
    public:
        FurrBall(const FurrBall& cpy) = delete;
        FurrBall(FurrBall&& mv)noexcept {
            //std::swap(options, mv.options);
            //ARC = std::move(mv.ARC);
        }
        /**
        * @brief Constructs a DB and allocates the Cache (with it's pages).
        * 
        * Uses Paging to avoid loading the entire DB in memory and only loads following the ARC eviction policy
        * @param DBpath the Path to create (or load) a DB.
        * @param PageSize The Page size in bytes. 
        * This acts as a hint to calculate the optimal Page size, the result will be equal or larger than the specified size.
        * Set to 0 if you want the library to decide the optimal size.
        * MUST BE POWER OF 2. 
        * @param numPages The number of pages to preallocate.
        * @param overwrite If DBpath points to an existing DB and this is true it will be overwritten instead of Loaded.
        * @see ARCPolicy
        */
        static FurrBall* CreateBall(std::string DBpath, size_t PageSize, size_t numPages, bool overwrite = false)noexcept;
        /**
         * Returns a pointer to the page that contains the vAddress. if vAddress is not found and is far from all pages available
         * Get() doesn't create an entry and considers the vAddress to be invalid to preserve "contingency".
         * 
         * @param vAddress a pointer to a virtual address used to index into the cache.
         * 
         * @returns a valid Pointer to memory on success or nullptr_t on error.
         */
        void* Get(void* vAddress)noexcept {
            //Snap to page border.
            //Query the Cache for the page.
            //if present return it
            //reload page from db and push into cache
        }
        /**
         * @brief Large data is stored seperate and a pointer to it is added to the cache
         * @param buffer The original data, a pointer to it is stored in the cache to avoid copying and moving data. Do not free.
         * @param size Buffer size.
         */
        void StoreLargeData(void* buffer, size_t size);

        const LockablePage* GenerateLockablePage() {

        }

        const Page* SetPageToLockable(Page* page) {

        }

        void LockPage(const Page* page) {
            if (!page->IsLockable()) {
                //We know the pages are not really const thus this cast is allowed.
                page = SetPageToLockable(const_cast<Page*>(page));
            }
        }
        /**
         * @brief Cleans up.
         */
        ~FurrBall()noexcept;
    };
}
