find_package(GTest CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_executable(FurrballsTests
    CMapTest.cpp
    PolicyTest.cpp
    ConcurrencyTest.cpp
    MissRatioTest.cpp
)

target_include_directories(FurrballsTests PRIVATE ${CMAKE_SOURCE_DIR}/Benchmark)
target_link_libraries(FurrballsTests PRIVATE FurrballsCore GTest::gtest_main Threads::Threads)

include(GoogleTest)
gtest_discover_tests(FurrballsTests DISCOVERY_TIMEOUT 60)
