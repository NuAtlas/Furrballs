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

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace Furrball {
	/**
	* @brief Returns the available memory, works for both Windows and Unix
	*/
	size_t GetAvailableMemory() {
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

	template<class Value>
	class CacheView {

	};

	/**
	 * @brief Caching Interface.
	 * @tparam Key Specifies Key Type.
	 * @tparam Value Specifies Value Type.
	 * @tparam Policy Specifies the eviction policy to be used. @see ARCPolicy
	 */
	template<class Key,class Value, template<class,class> class Policy>
	class Cache {
	private:
		Policy<Key, Value> policy;
		size_t CursorPosition = 0;
		unsigned int PageCount = 0;
		size_t PageSize = 0;
		std::mutex mutex;
	public:
		Cache() {
			
		}
		Cache& operator=(Cache&& mv) {
			mutex = std::move(mutex);
			std::swap(policy, mv.policy);
			std::swap(CursorPosition, mv.CursorPosition);
			std::swap(PageCount, mv.PageCount);
			std::swap(PageSize, mv.PageSize);
		};
		Cache(Cache&& move) {
			this->operator=(move);
		};
		/**
		 * @brief Cannot Copy Cache.
		 */
		Cache(const Cache&) = delete;
		/**
		 * @brief Cannot Copy Cache.
		 */
		Cache& operator=(const Cache& mv) = delete;
		~Cache() {
			//Cleanup 
		}
		void Expand() {
			//Maybe allocate another page? not sure for now.
		}
		size_t GetCapacity() {
			std::lock_guard(mutex);
			return PageSize * PageCount;
		}
		void Set(Key k, Value v) {

		}
		Value Get(Key k) {

		}
	};
	/**
	 * @brief Implements the ARC eviction policy
	 *
	 * You can create and manage your own cache separately by instantiating a Policy object and using it.
	 * @see S3FIFOPolicy
	 * @see LRUPolicy
	 * @see LFUPolicy
	 */
	template<class Key, class Value>
	class ARCPolicy {
	private:
		std::list<Key> t1;  // Recently added
		std::list<Key> t2;  // Recently used
		std::list<Key> b1;  // Ghost entries for t1
		std::list<Key> b2;  // Ghost entries for t2
		std::unordered_map<Key, Value> map;  // Key to value mapping
		size_t capacity;
		size_t p;  // Target size for t1

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
			if (t1.size() + b1.size() >= capacity) {
				if (t1.size() < capacity) {
					b1.pop_back();
				}
				else {
					t1.pop_back();
				}
			}
			if (t1.size() + t2.size() + b1.size() + b2.size() >= 2 * capacity) {
				if (t2.size() + b2.size() > capacity) {
					b2.pop_back();
				}
				else {
					t2.pop_back();
				}
			}
		}

	public:
		/**
		 * @brief Creates a cache following ARC policy.
		 * @param cap Capacity of the cache.
		 */
		ARCPolicy(size_t cap) : capacity(cap), p(0) {}
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
		friend class Cache<Key, Value, S3FIFOPolicy>;
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
		/*
		* @brief DB object. See RocksDB documentation
		*/
		rocksdb::DB* db = nullptr;
		rocksdb::Options options;
		struct Page {
			void* ptr;
			Page() {

			}
			~Page() {

			}
		};
		/**
		 * @brief Cache Object.
		 */
		ARCPolicy<size_t,void*> ARC = ARCPolicy<size_t,void*>();

		FurrBall() = default;
	public:
		FurrBall(const FurrBall& cpy) = delete;
		FurrBall(FurrBall&& mv) {
			std::swap(this->db, mv.db);
			std::swap(options, mv.options);
			FCache = std::move(mv.FCache);
		}
		/**
		* @brief Constructs a DB and allocates the Cache (with it's pages).
		* 
		* Uses Paging to avoid loading the entire DB in memory and only loads following the ARC eviction policy
		* @param DBpath the Path to create (or load) a DB.
		* @param BufferSize The buffer to allocate for read/write can be expanded.
		* @see ARCPolicy
		*/
		static FurrBall CreateBall(std::string DBpath, size_t BufferSize) {
			FurrBall fb;
			fb.options.compression = rocksdb::kLZ4Compression;
			fb.options.create_if_missing = true;
			rocksdb::Status status =
				rocksdb::DB::Open(fb.options, DBpath, &fb.db);
			if (status.ok());
			fb.options.create_if_missing = true;
			//Setup Cache.
			return fb;
		}
		/**
		 * @brief Cleans up.
		 */
		~FurrBall() {
			delete db;
		}
	};
}