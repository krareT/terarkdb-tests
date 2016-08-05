cmake_minimum_required(VERSION 3.5)

project(Terark_Engine_Test)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

aux_source_directory(./ SRC_LIST)

include_directories(./)
include_directories(./include)

include_directories(${TERARKDB_PATH}/include)
link_directories(${TERARKDB_PATH}/lib)
set(EXECUTABLE_OUTPUT_PATH ../../build)
add_executable(Terark_Engine_Test ${SRC_LIST} db/builder.cc db/c.cc db/dbformat.cc db/db_impl.cc
        db/db_iter.cc db/filename.cc db/log_reader.cc db/log_writer.cc db/memtable.cc db/repair.cc
        db/table_cache.cc db/version_edit.cc db/version_set.cc db/write_batch.cc table/block_builder.cc
        table/block.cc table/filter_block.cc table/format.cc table/iterator.cc table/merger.cc
        table/table_builder.cc table/table.cc table/two_level_iterator.cc util/arena.cc util/bloom.cc
        util/cache.cc util/coding.cc util/comparator.cc util/crc32c.cc util/env.cc util/env_posix.cc
        util/filter_policy.cc util/hash.cc util/histogram.cc util/logging.cc util/options.cc util/status.cc
        port/port_posix.cc ./util/testutil.cc src/Setting.cpp src/Setting.h src/leveldb.h
        src/TerarkBenchmark.cpp src/TerarkBenchmark.h src/WiredTigerBenchmark.cpp src/WiredTigerBenchmark.h
        src/TcpServer.cpp src/TcpServer.h
        )

target_link_libraries(Terark_Engine_Test
                        tbb
                        boost_system
                        boost_filesystem
                        terark-fsa_all-r
                        terark-db-r
                        wiredtiger
                        pthread
                        snappy)
