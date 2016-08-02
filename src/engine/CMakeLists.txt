cmake_minimum_required(VERSION 3.5)

project(Terark_Engine_Test)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(COMPILER g++)
    set(COMPILER_VERSION 5.3)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(COMPILER "Clang")
    set(COMPILER_VERSION 7.3)
endif()

set(CMAKE_CXX_COMPILER_VERSION )
set(CMAKE_CXX_COMPILER ${COMPILER})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(EXECUTABLE_OUTPUT_PATH ../../build)
set(CMAKE_LIBRARY_PATH ${TERARKDB_PATH}/lib)

aux_source_directory(./ SRC_LIST)

include_directories(./)
include_directories(./include)
include_directories(${TERARKDB_PATH}/include)

link_directories(${TERARKDB_PATH}/lib)

# find_library(wiredtiger REQUIRED)

add_executable(Terark_Engine_Test ${SRC_LIST} db/builder.cc db/c.cc db/dbformat.cc db/db_impl.cc
        db/db_iter.cc db/filename.cc db/log_reader.cc db/log_writer.cc db/memtable.cc db/repair.cc
        db/table_cache.cc db/version_edit.cc db/version_set.cc db/write_batch.cc table/block_builder.cc
        table/block.cc table/filter_block.cc table/format.cc table/iterator.cc table/merger.cc
        table/table_builder.cc table/table.cc table/two_level_iterator.cc util/arena.cc util/bloom.cc
        util/cache.cc util/coding.cc util/comparator.cc util/crc32c.cc util/env.cc util/env_posix.cc
        util/filter_policy.cc util/hash.cc util/histogram.cc util/logging.cc util/options.cc util/status.cc
        port/port_posix.cc ./util/testutil.h src/Setting.cpp src/Setting.h src/leveldb.h src/Benchmark.cpp src/Benchmark.h)

target_link_libraries(Terark_Engine_Test tbb boost_system boost_filesystem
        terark-fsa_all-${CMAKE_CXX_COMPILER}-${COMPILER_VERSION}-r
        terark-db-${CMAKE_CXX_COMPILER}-${COMPILER_VERSION}-r
        wiredtiger-2.8.0 pthread snappy)

