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
)

add_library(Furrballs STATIC ${SOURCES} ${HEADERS})

target_compile_options(Furrballs PUBLIC -mssse3 -mavx2 -mbmi2)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(lz4 CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)

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
