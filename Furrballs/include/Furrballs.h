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
#include <atomic>
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
        EvictionCallback evictionCallback = [](Key&) {};//NO-OP by default.

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

    struct FurrConfig {
        /**
         * @brief The limit size after which the AMP will not allocate more pages. 1MB by default
         */
        size_t CapacityLimit = 1024 * 1024 * sizeof(char);

        /**
         * @brief The starting number of pages. This is a hint to the allocator.
         */
        size_t InitialPageCount = 2;

        /**
         * @brief The size of each page. 4KB by default.
         */
        size_t PageSize = 4096;
        /**
         * @brief Sets the eviction callback.
         */
        ARCPolicy<size_t, void*>::EvictionCallback evictionCallback = [](size_t&) {};

        /**
         * @brief Sets the hash function for cache validation.
         */
        std::function<size_t(const void*, size_t)> hashFunction = nullptr;

        /**
         * @brief Sets the logging function.
         */
        std::function<void(const std::string&)> logFunction = nullptr;

        /**
         * @brief Sets the threshold for resizing the memory pool.
         */
        size_t ResizeThreshold = 4;

        /**
         * @brief The number of threads to use in burrst mode.
         */
        size_t BurrstThreadCount = 4;

        union {
            struct {
                /**
                 * @brief Indicates whether to use hybrid page sizes. false by default.
                 */
                bool UseHybridPages : 1;
                /**
                 * @brief Indicates whether the Furrballs are volatile.
                 * Volatile Furrballs are not persistent caches, meaning if the page is evicted, data is lost. You can use an eviction callback.
                 * false by default.
                 */
                bool IsVolatile : 1;
                /**
                 * @brief Indicates whether the pages are lockable. Lockable pages need to have a mutex, 
                 * consuming more memory per page entry and introducing more overhead. false by default.
                 */
                bool LockablePages : 1;
                /**
                 * @brief Enables or disables logging. false by default.
                 */
                bool EnableLogging : 1;
                /**
                 * @brief Enables or disables burst mode for parallel processing. false by default.
                 */
                bool EnableBurstMode : 1;
            };
            uint8_t flags = 0; // For convenience in handling all flags at once, 0 by default.
        };
    };

    /**
    * @class FurrBall
    * @brief Furrballs are a LZ4 Compressed DB using RocksDB with Cache and Paging Logic.
    */
    class FurrBall final {
    private:
        /**
         * @brief This struct is used to encapsulate implementation details and
         *        avoid requiring the user's build system to locate RocksDB headers.
         */
        struct ImplDetail;
        std::unique_ptr<ImplDetail> DataMembers;

        size_t PageSize;
        //Must remain POD.
        /**
         * \brief Page Metadata.
         */
        struct Page {
            void* ptr = nullptr;
            size_t PageIndex = 0;

            Page() {

            }
            virtual void* get(void* offset) {
                //it's the job of furrballs to validate offset passed here.
                return (char*)ptr + reinterpret_cast<size_t>(offset);
            }
            virtual bool IsLockable()const noexcept { return false; };

            ~Page() {

            }
        };
        struct LockablePage : public Page {
            std::mutex mutex;
            virtual bool IsLockable()const noexcept { return true; };
            virtual void* get(void* vptr) {
                std::lock_guard<std::mutex> lock(mutex);
                return Page::get(vptr);
            }
        };
        std::list<Page> PageList;

        /**
         * @brief AMP Expansion counter, when the counter reaches the threshold, live memory is expanded (amp_ExpansionMultiplier * page are allocated).
         *
         * See AMP in readme for description of the mechanism.
         */
        std::atomic_int amp_ExpansionCounter;

        /**
         * @brief .
         */
        std::atomic_int amp_ExpansionMultiplier = 1;

        const size_t SizeLimit = 1 * 1024 * 1024 * sizeof(char);

        /**
         * @brief Cache Object.
         */
        //ARCPolicy<size_t,void*> ARC = ARCPolicy<size_t,void*>();

        FurrBall(const FurrConfig& config)noexcept;
        FurrBall(std::unique_ptr<ImplDetail>)noexcept;

        void OnEvict(size_t key)noexcept;

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
        static FurrBall* CreateBall(const std::string& DBpath,const FurrConfig& config, bool overwrite = false)noexcept;
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