cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_COMPILER g++)
set(EXECUTABLE_OUTPUT_PATH ./build)

#if (${TERARKDB_PATH})
#else
#  set(TERARKDB_PATH $ENV{HOME}/Documents/pkg/terichdb-Linux-x86_64-g++-5.4-bmi2-1/)
#endif

#if (${TERARKDB_ROCKS})
#else
  #set(TERARKDB_ROCKS rocksdb)
#endif

set(TERARKDB_ROCKS $ENV{PWD}/../rocksdb)

project(TerarkDB_Test_Framework)
add_subdirectory(./src/engine)
