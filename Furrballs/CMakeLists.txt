set(SOURCES
    src/Furrballs.cpp
    src/Logger.cpp
    src/Page.cpp
    src/NodeJob.cpp
)

set(HEADERS
    include/Furrballs.h
    include/Logger.h
    include/MemoryManager.h
    include/Error.h
    include/Page.h
    include/NodeJob.h
    include/WaitGroup.h
    include/CMap.h
    include/Remarc.h
    include/Policy.h
)

add_library(Furrballs STATIC ${SOURCES} ${HEADERS})

target_compile_options(Furrballs PUBLIC -mssse3 -mavx2 -mbmi2)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(lz4 CONFIG QUIET)
if(NOT lz4_FOUND)
    find_library(LZ4_LIBRARY NAMES lz4)
    find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
    add_library(lz4::lz4 STATIC IMPORTED)
    set_target_properties(lz4::lz4 PROPERTIES
        IMPORTED_LOCATION "${LZ4_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
endif()

find_package(RocksDB CONFIG QUIET)
if(NOT RocksDB_FOUND)
    find_library(ROCKSDB_LIBRARY NAMES rocksdb)
    find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
    add_library(RocksDB::rocksdb STATIC IMPORTED)
    set_target_properties(RocksDB::rocksdb PROPERTIES
        IMPORTED_LOCATION "${ROCKSDB_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIR}")
endif()

find_package(xxHash CONFIG QUIET)
if(NOT xxHash_FOUND)
    find_library(XXHASH_LIBRARY NAMES xxhash)
    find_path(XXHASH_INCLUDE_DIR NAMES xxhash.h)
    add_library(xxHash::xxhash STATIC IMPORTED)
    set_target_properties(xxHash::xxhash PROPERTIES
        IMPORTED_LOCATION "${XXHASH_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIR}")
endif()

target_link_libraries(Furrballs PRIVATE lz4::lz4 RocksDB::rocksdb xxHash::xxhash Numatic)
target_include_directories(Furrballs
PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(Furrballs PUBLIC ${CMAKE_SOURCE_DIR}/numatic/include)

if(SIMULATE_NUMA_LATENCY_NS GREATER 0)
    target_compile_definitions(Furrballs PUBLIC SIMULATE_NUMA_LATENCY_NS=${SIMULATE_NUMA_LATENCY_NS})
    message(STATUS "NUMA latency simulation enabled: ${SIMULATE_NUMA_LATENCY_NS}ns")
endif()
