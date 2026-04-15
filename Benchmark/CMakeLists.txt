add_executable(Benchmark "Benchmark.cpp")
target_link_libraries(Benchmark PRIVATE Furrballs)

add_executable(BaselineBenchmark "BaselineBenchmark.cpp")
target_include_directories(BaselineBenchmark PRIVATE "${CMAKE_SOURCE_DIR}/Furrballs/include")
find_package(Threads REQUIRED)
target_link_libraries(BaselineBenchmark PRIVATE Threads::Threads)
