cmake_minimum_required(VERSION 3.5)
project(scan_perf)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_VERBOSE_MAKEFILE on)
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb ")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

add_executable(scan_perf main.cpp
                         rocksdb.h
                         rocksdb.cpp)

include_directories(third/rocksdb/include
                    third/benchmark/include)

target_link_libraries(scan_perf
        third/rocksdb/librocksdb.a
        third/benchmark/build/src/libbenchmark.a
        pthread
        zstd
        lz4
        bz2
        snappy
        z)