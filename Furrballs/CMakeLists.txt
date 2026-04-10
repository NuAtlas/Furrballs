add_library(Furrballs STATIC "src/Furrballs.cpp" "include/Furrballs.h" "include/MemoryManager.h" "include/Logger.h" "include/IFactory.h")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SOURCES
    src/Furrballs.cpp
)

set(HEADERS
    include/Furrballs.h
    include/IFactory.h
    include/Logger.h
    include/MemoryManager.h
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(lz4 CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")

target_link_libraries(Furrballs PRIVATE lz4::lz4 RocksDB::rocksdb)
target_include_directories(Furrballs
PUBLIC
    ${CMAKE_SOURCE_DIR}/Furrballs/include
PRIVATE
    ${CMAKE_SOURCE_DIR}/Furrballs/src
)