add_executable(Benchmark "Benchmark.cpp")
target_link_libraries(Benchmark PRIVATE Furrballs FurrballsSM)
target_include_directories(Benchmark PRIVATE "${CMAKE_SOURCE_DIR}/numatic/include")

add_executable(BaselineBenchmark "BaselineBenchmark.cpp")
target_include_directories(BaselineBenchmark PRIVATE "${CMAKE_SOURCE_DIR}/Furrballs/include")
find_package(Threads REQUIRED)
target_link_libraries(BaselineBenchmark PRIVATE Threads::Threads)

add_executable(IsolatedBench "IsolatedBench.cpp")
target_include_directories(IsolatedBench PRIVATE "${CMAKE_SOURCE_DIR}/Furrballs/include" "${CMAKE_SOURCE_DIR}/numatic/include")
target_link_libraries(IsolatedBench PRIVATE Furrballs Numatic Threads::Threads)
