// Furrballs.h : Header file for your target.
#pragma once
#include <memory>
#include <string>
#include <filesystem>
#include <thread>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/advanced_cache.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace Furrball {
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
	class FurrBall final {
	private:
		static rocksdb::DB* db;
		static rocksdb::Options options;
	public:
		static void Initialize() {
			options.create_if_missing = true;
		}
	};
}