add_library(Furrballs STATIC "src/Furrballs.cpp" "include/Furrballs.h")

#set(CMAKE_CXX_STANDARD_REQUIRED ON)

# List source files
set(SOURCES
    src/Furrballs.cpp
)

# List header files (optional)
set(HEADERS
    include/Furrballs.h
    include/IFactory.h
)
# Set the Visual Studio folder structure
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/src PREFIX "Source Files" FILES ${SOURCES})
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/include PREFIX "Header Files" FILES ${HEADERS})

find_package(lz4 CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")

# Link to library
target_link_libraries(Furrballs PRIVATE lz4::lz4)
target_link_libraries(Furrballs PRIVATE RocksDB::rocksdb)
target_include_directories(Furrballs 
PUBLIC
    ${CMAKE_SOURCE_DIR}/Furrballs/include
PRIVATE
    ${CMAKE_SOURCE_DIR}/Furrballs/src
)