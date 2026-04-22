set(SOURCES
    src/SharedNothingCache.cpp
)

set(HEADERS
    include/SharedNothingCache.h
)

add_library(FurrballsSM STATIC ${SOURCES} ${HEADERS})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(RocksDB CONFIG REQUIRED)

target_link_libraries(FurrballsSM PRIVATE RocksDB::rocksdb Numatic)
target_include_directories(FurrballsSM
PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/Furrballs/include
PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(FurrballsSM PUBLIC ${CMAKE_SOURCE_DIR}/numatic/include)

if(SIMULATE_NUMA_LATENCY_NS GREATER 0)
    target_compile_definitions(FurrballsSM PUBLIC SIMULATE_NUMA_LATENCY_NS=${SIMULATE_NUMA_LATENCY_NS})
endif()
