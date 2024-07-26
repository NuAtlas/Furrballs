add_library(Furrballs STATIC "src/Furrballs.cpp" "include/Furrballs.h")

find_package(lz4 CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
message(STATUS $ENV{VCPKG_ROOT})
message(STATUS "RocksDB include directories: ${RocksDB_INCLUDE_DIRS}")
message(STATUS "LZ4 include directories: ${LZ4_INCLUDE_DIRS}")
# Link to library
target_link_libraries(Furrballs PRIVATE lz4::lz4)
target_link_libraries(Furrballs PRIVATE RocksDB::rocksdb)
target_include_directories(Furrballs PRIVATE
    ${CMAKE_SOURCE_DIR}/Furrballs/include
)