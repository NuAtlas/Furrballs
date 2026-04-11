set(SOURCES
    src/Furrballs.cpp
    src/Logger.cpp
)

set(HEADERS
    include/Furrballs.h
    include/IFactory.h
    include/Logger.h
    include/MemoryManager.h
    include/Error.h
)

add_library(Furrballs STATIC ${SOURCES} ${HEADERS})

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(lz4 CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)

target_link_libraries(Furrballs PRIVATE lz4::lz4 RocksDB::rocksdb)
target_include_directories(Furrballs
PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
