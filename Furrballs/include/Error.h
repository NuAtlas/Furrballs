#pragma once

namespace NuAtlas
{
    //C-style Enum, with spilling
        enum Error
    {
        // General
        NO_ERR              = 0,
        UNKNOWN_ERR         = 1,
        NOT_IMPLEMENTED     = 2,
        INVALID_ARG         = 3,
        INVALID_REQ         = 4,
        ALREADY_EXISTS      = 5,
        NOT_FOUND           = 6,
        // Initialization / Lifecycle
        NOT_INITIALIZED     = 10,
        ALREADY_INITIALIZED = 11,
        ALREADY_SHUTDOWN    = 12,
        INVALID_STATE       = 13,
        // Configuration
        INVALID_CONFIG      = 20,
        CAPACITY_ZERO       = 21,
        PAGE_SIZE_INVALID   = 22,
        UNSUPPORTED_FLAG    = 23,
        // Memory
        OUT_OF_MEM          = 30,
        ALLOC_FAILED        = 31,
        NUMA_ALLOC_FAILED   = 32,
        NUMA_NOT_AVAILABLE  = 33,
        ALIGNMENT_FAILED    = 34,
        PROTECT_FAILED      = 35,
        DOUBLE_FREE         = 36,
        INVALID_PTR         = 37,
        BUF_NOT_LARGE_ENOUGH = 38,
        // Cache / Eviction
        CACHE_FULL          = 40,
        CACHE_MISS          = 41,
        KEY_DUPLICATE       = 42,
        KEY_NOT_FOUND       = 43,
        EVICTION_FAILED     = 44,
        EVICTION_CALLBACK_ERR = 45,
        // Storage / RocksDB
        DB_OPEN_FAILED      = 50,
        DB_CLOSE_FAILED     = 51,
        DB_WRITE_FAILED     = 52,
        DB_READ_FAILED      = 53,
        DB_DELETE_FAILED    = 54,
        DB_FLUSH_FAILED     = 55,
        DB_CORRUPTED        = 56,
        DB_COMPACT_FAILED   = 57,
        // Compression / LZ4
        COMPRESS_FAILED     = 60,
        DECOMPRESS_FAILED   = 61,
        COMPRESS_BUF_TOO_SMALL = 62,
        // Threading / Synchronization
        LOCK_TIMEOUT        = 70,
        LOCK_ACQUIRE_FAILED = 71,
        // I/O / Filesystem
        IO_READ_FAILED      = 80,
        IO_WRITE_FAILED     = 81,
        PATH_NOT_FOUND      = 82,
        PERMISSION_DENIED   = 83,
    };
} // namespace NuAtlas
