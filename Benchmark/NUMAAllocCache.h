#pragma once
#include "SeqLock.h"
#include "Error.h"
#include "Numatic.h"
#include <unordered_map>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <cstring>
#include <atomic>

class NUMAAllocCache {
    struct BumpPage {
        void* data;
        size_t pageSize;
        int nodeId;
        std::atomic<size_t> usedBytes{0};

        BumpPage(size_t ps, int node) : pageSize(ps), nodeId(node) {
            data = NuAtlas::Numatic::AllocateOnNode(ps, node);
        }
        ~BumpPage() { if(data) NuAtlas::Numatic::FreeNUMA(data, pageSize); }

        void* tryBump(size_t size) {
            size_t old = usedBytes.load(std::memory_order_relaxed);
            size_t aligned;
            do {
                aligned = (old + 7) & ~7ULL;
                if (aligned + size > pageSize) return nullptr;
            } while (!usedBytes.compare_exchange_weak(old, aligned + size,
                         std::memory_order_acq_rel, std::memory_order_relaxed));
            return static_cast<char*>(data) + aligned;
        }
    };

    struct Meta {
        void* dataOffset = nullptr;
        size_t dataSize = 0;
    };

    std::shared_mutex mutex;
    std::unordered_map<std::string, std::unique_ptr<StreamLine::SeqLock<Meta>>> store;
    std::vector<std::unique_ptr<BumpPage>> pages;
    std::vector<size_t> currentPagePerNode;
    size_t pageSize;
    int nodeCount;
    std::atomic<int> rrNode{0};

public:
    NUMAAllocCache(size_t numPages, size_t ps, int nodes) : pageSize(ps), nodeCount(nodes), currentPagePerNode(nodes, 0) {
        for (int n = 0; n < nodes; n++) {
            for (size_t i = 0; i < numPages / nodes; i++) {
                pages.push_back(std::make_unique<BumpPage>(ps, n));
            }
        }
    }

    NuAtlas::Error Set(const std::string& key, void* data, size_t size) {
        if (!data || !size) return NuAtlas::INVALID_ARG;
        std::unique_lock lock(mutex);

        auto it = store.find(key);
        if (it != store.end()) {
            Meta m = it->second->Read();
            if (size <= m.dataSize) {
                std::memcpy(m.dataOffset, data, size);
                m.dataSize = size;
                it->second->Write(m);
                return NuAtlas::NO_ERR;
            }
        }

        int targetNode = rrNode.fetch_add(1, std::memory_order_relaxed) % nodeCount;
        void* loc = nullptr;
        size_t& cpn = currentPagePerNode[targetNode];
        for (size_t attempt = 0; attempt < pages.size(); attempt++) {
            size_t idx = (cpn + attempt) % pages.size();
            if (pages[idx]->nodeId == targetNode) {
                loc = pages[idx]->tryBump(size);
                if (loc) { cpn = idx; break; }
            }
        }
        if (!loc) {
            for (size_t idx = 0; idx < pages.size(); idx++) {
                loc = pages[idx]->tryBump(size);
                if (loc) { cpn = idx; break; }
            }
        }
        if (!loc) return NuAtlas::OUT_OF_MEM;

        std::memcpy(loc, data, size);
        Meta m{loc, size};
        if (it != store.end()) {
            it->second->Write(m);
        } else {
            store.insert({key, std::make_unique<StreamLine::SeqLock<Meta>>(m)});
        }
        return NuAtlas::NO_ERR;
    }

    NuAtlas::Error Get(const std::string& key, void* outBuf, size_t bufSize, size_t& outSize) {
        if (key.empty()) return NuAtlas::INVALID_ARG;
        auto it = store.find(key);
        if (it == store.end()) return NuAtlas::INVALID_ARG;
        Meta m = it->second->Read();
        outSize = m.dataSize;
        if (bufSize < m.dataSize) return NuAtlas::BUF_NOT_LARGE_ENOUGH;
        std::memcpy(outBuf, m.dataOffset, m.dataSize);
        return NuAtlas::NO_ERR;
    }
};
