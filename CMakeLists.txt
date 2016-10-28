cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_COMPILER g++)
set(EXECUTABLE_OUTPUT_PATH ./build)

set(TERARKDB_PATH /home/terark0/Documents/pkg/terark-db-Linux-x86_64-g++-5.3-bmi2-1/ )
set(TERARKDB_ROCKS /home/terark0/Documents/pkg/terark-zip-rocksdb-Linux-x86_64-g++-5.3-bmi2-1/ )

project(TerarkDB_Test_Framework)
add_subdirectory(./src/engine)
