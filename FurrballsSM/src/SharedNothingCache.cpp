#include "SharedNothingCache.h"
#include "Logger.h"
#include "MemoryManager.h"
#include <rocksdb/db.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/cache.h>
#include <cstring>
#include <unordered_map>
#include <memory>
#include <thread>

using namespace NuAtlas;
using namespace NuAtlas::SM;

static constexpr size_t SM_KEY_BUF = 128;
static constexpr size_t SM_DATA_BUF = 4096;
static constexpr size_t SM_QUEUE_DEPTH = 256;

enum SlotPhase : uint32_t {
    SLOT_FREE = 0,
    SLOT_CLAIMED = 1,
    SLOT_SUBMITTED = 2,
    SLOT_COMPLETED = 3
};

struct alignas(128) RequestSlot {
    std::atomic<uint32_t> phase{SLOT_FREE};
    uint8_t opType;
    char keyBuf[SM_KEY_BUF];
    size_t keyLen;
    char dataBuf[SM_DATA_BUF];
    size_t dataSize;
    size_t outSize;
    Error result;
};

struct SMNodeState {
    Numatic::NumaLocalMemoryResource nodeMR;
    std::pmr::vector<Page> pages;
    using KeyLock = StreamLine::SeqLock<SharedNothingCache::KeyMeta>;
    std::unordered_map<std::string, std::unique_ptr<KeyLock>> keyStore;
    std::shared_mutex rwMutex;
    size_t currentPage = 0;
    void* physicalMemory = nullptr;

    SMNodeState() : pages(&nodeMR) {}
};

struct SMNodeWorker {
    SMNodeState* state = nullptr;
    std::thread thread;
    std::atomic<bool> running{false};
    RequestSlot* incoming = nullptr;
    size_t numSlots = 0;
    int nodeId = -1;
};

struct SharedNothingCache::Impl {
    rocksdb::DB* db = nullptr;
    SMNodeState** nodes = nullptr;
    SMNodeWorker* workers = nullptr;
    int nodeCount = 0;
    size_t pageSize = 0;
    size_t numPages = 0;
    bool isVolatile = false;
};

static void workerLoopMPSC(SMNodeWorker* w) {
    Numatic::PinCurrentThreadToNode(w->nodeId);
    Logger::getInstance().info("SM worker started on node " + std::to_string(w->nodeId));

    while (w->running.load(std::memory_order_relaxed)) {
        bool found = false;
        for (size_t i = 0; i < w->numSlots; i++) {
            RequestSlot& slot = w->incoming[i];
            if (slot.phase.load(std::memory_order_acquire) != SLOT_SUBMITTED) continue;

            found = true;
            std::string key(slot.keyBuf, slot.keyLen);

            if (slot.opType == 0) {
                auto it = w->state->keyStore.find(key);
                if (it != w->state->keyStore.end()) {
                    SharedNothingCache::KeyMeta meta = it->second->Read();
                    size_t copySize = meta.DataSize < SM_DATA_BUF ? meta.DataSize : SM_DATA_BUF;
                    std::memcpy(slot.dataBuf, meta.DataOffset, copySize);
                    slot.outSize = meta.DataSize;
                    slot.result = NO_ERR;
                } else {
                    slot.result = INVALID_ARG;
                    slot.outSize = 0;
                }
            }

            slot.phase.store(SLOT_COMPLETED, std::memory_order_release);
        }
        if (!found) {
            std::this_thread::yield();
        }
    }
}

SharedNothingCache::SharedNothingCache(const SMConfig& config, size_t numPages) noexcept
    : pImpl(new Impl())
{
    pImpl->pageSize = config.PageSize;
    pImpl->numPages = numPages;
    pImpl->isVolatile = config.IsVolatile;
}

void SharedNothingCache::Bootstrap() {}

void SharedNothingCache::Shutdown() {}

SharedNothingCache* SharedNothingCache::Create(const std::string& dbPath, const SMConfig& config) noexcept {
    if (!Numatic::IsNUMAAvailable()) return nullptr;

    int nodeCount = Numatic::GetNodeCount();
    if (nodeCount < 1) return nullptr;

    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions tableOpts;
    tableOpts.block_cache = rocksdb::NewLRUCache(0);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tableOpts));
    options.create_if_missing = true;

    rocksdb::DB* db = nullptr;
    auto status = rocksdb::DB::Open(options, dbPath, &db);
    if (!status.ok()) return nullptr;

    size_t totalPhysSize = config.PageSize * config.InitialPageCount;
    size_t numPages = config.InitialPageCount;

    SharedNothingCache* cache = new SharedNothingCache(config, numPages);
    cache->pImpl->db = db;
    cache->pImpl->nodeCount = nodeCount;
    cache->pImpl->nodes = (SMNodeState**)malloc(sizeof(SMNodeState*) * nodeCount);
    cache->pImpl->workers = new SMNodeWorker[nodeCount];

    for (int i = 0; i < nodeCount; i++) {
        void* phys = Numatic::AllocateOnNode(totalPhysSize, i);
        if (!phys) {
            for (int j = 0; j < i; j++) {
                Numatic::FreeNUMA(cache->pImpl->nodes[j]->physicalMemory, totalPhysSize);
                cache->pImpl->nodes[j]->~SMNodeState();
                Numatic::FreeNUMA(cache->pImpl->nodes[j], sizeof(SMNodeState));
            }
            free(cache->pImpl->nodes);
            delete[] cache->pImpl->workers;
            delete db;
            delete cache;
            return nullptr;
        }

        void* auxD = Numatic::AllocateOnNode(sizeof(SMNodeState), i);
        SMNodeState* ns = new(auxD) SMNodeState();
        ns->physicalMemory = phys;
        ns->pages.reserve(numPages);
        for (size_t p = 0; p < numPages; p++) {
            ns->pages.emplace_back(static_cast<char*>(phys) + (config.PageSize * p), config.PageSize, p);
        }
        cache->pImpl->nodes[i] = ns;

        SMNodeWorker& w = cache->pImpl->workers[i];
        w.state = ns;
        w.nodeId = i;
        w.numSlots = SM_QUEUE_DEPTH;
        w.incoming = new RequestSlot[SM_QUEUE_DEPTH]();
        w.running.store(true, std::memory_order_relaxed);
    }

    for (int i = 0; i < nodeCount; i++) {
        cache->pImpl->workers[i].thread = std::thread(workerLoopMPSC, &cache->pImpl->workers[i]);
    }

    return cache;
}

Error SharedNothingCache::Set(const std::string& key, void* data, size_t size) noexcept {
    if (!data || !size || key.empty()) return INVALID_ARG;

    int localNode = Numatic::GetCurrentNode();
    SMNodeState* ns = pImpl->nodes[localNode];
    std::unique_lock<std::shared_mutex> lock(ns->rwMutex);

    auto it = ns->keyStore.find(key);
    if (it == ns->keyStore.end()) {
        void* loc = nullptr;
        while (ns->currentPage < ns->pages.size()) {
            loc = ns->pages[ns->currentPage].TryBump(size);
            if (loc) break;
            ns->currentPage++;
        }
        if (!loc) return OUT_OF_MEM;

        std::memcpy(loc, data, size);
        KeyMeta meta{ns->currentPage, size, loc, localNode};
        ns->keyStore.insert({key, std::make_unique<SMNodeState::KeyLock>(meta)});
        Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
    } else {
        KeyMeta meta = it->second->Read();
        if (size <= meta.DataSize) {
            std::memcpy(meta.DataOffset, data, size);
            meta.DataSize = size;
            it->second->Write(meta);
        } else {
            void* loc = nullptr;
            while (ns->currentPage < ns->pages.size()) {
                loc = ns->pages[ns->currentPage].TryBump(size);
                if (loc) break;
                ns->currentPage++;
            }
            if (!loc) return OUT_OF_MEM;
            std::memcpy(loc, data, size);
            meta.PageIndex = ns->currentPage;
            meta.DataOffset = loc;
            meta.DataSize = size;
            it->second->Write(meta);
            Stats.BytesWritten.fetch_add(size, std::memory_order_relaxed);
        }
    }
    return NO_ERR;
}

Error SharedNothingCache::Get(const std::string& key, void* outBuf, size_t bufSize, size_t& outSize) noexcept {
    if (key.empty()) return INVALID_ARG;

    int localNode = Numatic::GetCurrentNode();

    SMNodeState* localNs = pImpl->nodes[localNode];
    {
        auto it = localNs->keyStore.find(key);
        if (it != localNs->keyStore.end()) {
            KeyMeta meta = it->second->Read();
            outSize = meta.DataSize;
            if (bufSize < meta.DataSize) return BUF_NOT_LARGE_ENOUGH;
            std::memcpy(outBuf, meta.DataOffset, meta.DataSize);
            Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
            Stats.BytesRead.fetch_add(meta.DataSize, std::memory_order_relaxed);
            Stats.LocalHitCount.fetch_add(1, std::memory_order_relaxed);
            return NO_ERR;
        }
    }

    for (int n = 0; n < pImpl->nodeCount; n++) {
        if (n == localNode) continue;

        SMNodeWorker& worker = pImpl->workers[n];
        RequestSlot* slot = nullptr;

        for (size_t i = 0; i < worker.numSlots; i++) {
            uint32_t p = worker.incoming[i].phase.load(std::memory_order_acquire);
            if (p == SLOT_FREE) {
                uint32_t expected = SLOT_FREE;
                if (worker.incoming[i].phase.compare_exchange_strong(
                        expected, SLOT_CLAIMED, std::memory_order_acq_rel)) {
                    slot = &worker.incoming[i];
                    break;
                }
            }
        }

        if (!slot) continue;

        slot->opType = 0;
        size_t kLen = key.size() < SM_KEY_BUF ? key.size() : SM_KEY_BUF - 1;
        std::memcpy(slot->keyBuf, key.data(), kLen);
        slot->keyLen = kLen;
        slot->dataSize = 0;
        slot->outSize = 0;

        slot->phase.store(SLOT_SUBMITTED, std::memory_order_release);

        while (slot->phase.load(std::memory_order_acquire) != SLOT_COMPLETED) {
            std::this_thread::yield();
        }

        Error res = slot->result;
        if (res == NO_ERR) {
            outSize = slot->outSize;
            if (bufSize < slot->outSize) {
                slot->phase.store(SLOT_FREE, std::memory_order_release);
                return BUF_NOT_LARGE_ENOUGH;
            }
            std::memcpy(outBuf, slot->dataBuf, slot->outSize);
            Stats.HitCount.fetch_add(1, std::memory_order_relaxed);
            Stats.BytesRead.fetch_add(slot->outSize, std::memory_order_relaxed);
            slot->phase.store(SLOT_FREE, std::memory_order_release);
            return NO_ERR;
        }

        slot->phase.store(SLOT_FREE, std::memory_order_release);
    }

    Stats.MissCount.fetch_add(1, std::memory_order_relaxed);
    return INVALID_ARG;
}

SharedNothingCache::~SharedNothingCache() noexcept {
    if (!pImpl) return;

    for (int i = 0; i < pImpl->nodeCount; i++) {
        pImpl->workers[i].running.store(false, std::memory_order_relaxed);
    }
    for (int i = 0; i < pImpl->nodeCount; i++) {
        if (pImpl->workers[i].thread.joinable()) {
            pImpl->workers[i].thread.join();
        }
    }
    delete[] pImpl->workers;

    for (int i = 0; i < pImpl->nodeCount; i++) {
        if (pImpl->nodes[i]) {
            size_t physSize = pImpl->pageSize * pImpl->numPages;
            Numatic::FreeNUMA(pImpl->nodes[i]->physicalMemory, physSize);
            pImpl->nodes[i]->~SMNodeState();
            Numatic::FreeNUMA(pImpl->nodes[i], sizeof(SMNodeState));
        }
    }
    free(pImpl->nodes);

    if (pImpl->db) {
        pImpl->db->Close();
        delete pImpl->db;
    }
    delete pImpl;
}
