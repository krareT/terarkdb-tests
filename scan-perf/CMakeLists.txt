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
        "-Wl,--whole-archive"
        /home/wangfo/src/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-0/lib_static/libterark-zip-rocksdb-r.a
        /home/wangfo/src/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-0/lib_static/libterark-zbs-r.a
        /home/wangfo/src/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-0/lib_static/libterark-fsa-r.a
        /home/wangfo/src/terark-zip-rocksdb/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-0/lib_static/libterark-core-r.a
        "-Wl,--no-whole-archive"
        gomp
        ${CMAKE_SOURCE_DIR}/third/rocksdb/librocksdb.a
        ${CMAKE_SOURCE_DIR}/third/benchmark/build/src/libbenchmark.a
        pthread
        zstd
        lz4
        bz2
        snappy
        z)